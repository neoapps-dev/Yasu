use std::env;
use std::io::{self, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::io::FromRawFd;
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::thread;
use std::fs;
use std::time::Duration;
use std::path::Path;
use nix::pty::{openpty, Winsize};
use nix::unistd::{isatty, Pid};
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use libc;

const DAEMON_PID_FILE: &str = "/tmp/yasu_daemon.pid";
const SOCKET_PATH: &str = "/tmp/yasu.sock";

fn main() {
    let args: Vec<String> = env::args().collect();    
    if args.get(1).map(String::as_str).unwrap_or("") == "--daemon" {
        run_daemon();
    } else {
        run_client(&args[1..]);
    }
}

fn run_daemon() {
    let pid = std::process::id().to_string();
    fs::write(DAEMON_PID_FILE, pid).expect("Failed to write PID file");
    if Path::new(SOCKET_PATH).exists() {
        fs::remove_file(SOCKET_PATH).expect("Failed to remove existing socket file");
    }
    
    let listener = UnixListener::bind(SOCKET_PATH).expect("Failed to bind to socket");
    fs::set_permissions(SOCKET_PATH, fs::Permissions::from_mode(0o777))
        .expect("Failed to set socket permissions");
        
    println!("Yasu daemon started and listening on {}", SOCKET_PATH);
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(|| handle_client(stream));
            }
            Err(e) => {
                eprintln!("Error accepting connection: {}", e);
            }
        }
    }
}

fn handle_client(mut stream: UnixStream) {
    let mut buffer = [0; 1024];
    let mut all_data = Vec::new();
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => {
                all_data.extend_from_slice(&buffer[..n]);
                if n < buffer.len() {
                    break;
                }
            }
            Err(e) => {
                eprintln!("Error reading from stream: {}", e);
                return;
            }
        }
    }
    
    let request = String::from_utf8_lossy(&all_data);
    let mut lines = request.split('\n');
    let command = match lines.next() {
        Some(cmd) => cmd,
        None => {
            let _ = stream.write_all(b"Invalid request format");
            return;
        }
    };
    
    let args_line = lines.next().unwrap_or("");
    let args: Vec<&str> = if !args_line.is_empty() {
        args_line.split('\0').collect()
    } else {
        Vec::new()
    };
    
    let interactive_flag = lines.next().unwrap_or("");
    let use_pty = interactive_flag == "interactive";
    if use_pty {
        handle_pty_command(&stream, command, &args);
    } else {
        let mut cmd = Command::new(command);
        cmd.args(&args)
           .stdout(Stdio::piped())
           .stderr(Stdio::piped());
        
        match cmd.output() {
            Ok(output) => {
                let _ = stream.write_all(&output.stdout);
                let _ = stream.write_all(&output.stderr);
            }
            Err(e) => {
                let _ = stream.write_all(format!("Failed to execute command: {}", e).as_bytes());
            }
        }
    }
}

fn handle_pty_command(mut stream: &UnixStream, command: &str, args: &[&str]) {
    let rows = 24;
    let cols = 80;
    let pty = match openpty(&Winsize {
        ws_row: rows,
        ws_col: cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    }, None) {
        Ok(pty) => pty,
        Err(e) => {
            let _ = stream.write_all(format!("Failed to open pty: {}", e).as_bytes());
            return;
        }
    };
    
    let pty_master_fd = pty.master;
    let pty_slave_fd = pty.slave;
    let child_exited = Arc::new(AtomicBool::new(false));
    let child_exited_clone = child_exited.clone();
    let child_exited_for_pty = child_exited.clone();
    match unsafe { libc::fork() } {
        -1 => {
            let _ = stream.write_all(b"Failed to fork process");
            return;
        },
        0 => {
            unsafe {
                libc::close(pty_master_fd);
                if libc::setsid() < 0 {
                    libc::_exit(1);
                }
                
                if libc::ioctl(pty_slave_fd, libc::TIOCSCTTY, 0) < 0 {
                    libc::_exit(1);
                }
                
                libc::dup2(pty_slave_fd, libc::STDIN_FILENO);
                libc::dup2(pty_slave_fd, libc::STDOUT_FILENO);
                libc::dup2(pty_slave_fd, libc::STDERR_FILENO);
                if pty_slave_fd > 2 {
                    libc::close(pty_slave_fd);
                }
                
                let mut cmd = Command::new(command);
                cmd.args(args)
                   .env("TERM", "xterm-256color");
                
                let error = cmd.exec();
                eprintln!("Failed to exec: {}", error);
                libc::_exit(1);
            }
        },
        pid => {
            unsafe { libc::close(pty_slave_fd); }
            let pid = Pid::from_raw(pid);
            let pty_master = unsafe { std::fs::File::from_raw_fd(pty_master_fd) };
            let mut pty_reader = pty_master.try_clone().expect("Failed to clone pty master");
            let mut pty_writer = pty_master;
            let stream_clone = stream.try_clone().expect("Failed to clone stream");
            let mut stream_reader = stream.try_clone().expect("Failed to clone stream");
            let mut stream_writer = stream_clone;
            let pty_to_stream = thread::spawn(move || {
                let mut buffer = [0; 1024];
                while !child_exited_for_pty.load(Ordering::SeqCst) {
                    match pty_reader.read(&mut buffer) {
                        Ok(0) => break,
                        Ok(n) => {
                            if let Err(_) = stream_writer.write_all(&buffer[..n]) {
                                break;
                            }
                            let _ = stream_writer.flush();
                        }
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(10));
                            continue;
                        }
                        Err(e) => {
                            if e.kind() != io::ErrorKind::Interrupted {
                                break;
                            }
                        }
                    }
                }
            });

            let stream_to_pty = thread::spawn(move || {
                let mut buffer = [0; 1024];
                while !child_exited_clone.load(Ordering::SeqCst) {
                    match stream_reader.read(&mut buffer) {
                        Ok(0) => break,
                        Ok(n) => {
                            if let Err(_) = pty_writer.write_all(&buffer[..n]) {
                                break;
                            }
                            let _ = pty_writer.flush();
                        }
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(10));
                            continue;
                        }
                        Err(e) => {
                            if e.kind() != io::ErrorKind::Interrupted {
                                break;
                            }
                        }
                    }
                }
            });

            let mut status = 0;
            unsafe {
                libc::waitpid(pid.as_raw(), &mut status, 0);
            }
            
            child_exited.store(true, Ordering::SeqCst);
            let _ = pty_to_stream.join();
            let _ = stream_to_pty.join();
        }
    }
}

fn run_client(args: &[String]) {
    let mut stream = match UnixStream::connect(SOCKET_PATH) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to connect to daemon: {}", e);
            eprintln!("Make sure the daemon is running with 'yasu --daemon'");
            return;
        }
    };
    let command = args.get(0).map(String::as_str).unwrap_or("su");
    let mut command_args: &[String] = &[];
    if args.len() >= 1 {
        command_args = &args[1..];
    }
    let interactive = atty::is(atty::Stream::Stdin) && atty::is(atty::Stream::Stdout);
    let mut request = String::new();
    request.push_str(command);
    request.push('\n');
    if !command_args.is_empty() {
        request.push_str(&command_args.join("\0"));
    }
    request.push('\n');
    if interactive {
        request.push_str("interactive\n");
    }
    stream.write_all(request.as_bytes()).expect("Failed to send request");
    if interactive {
        handle_interactive_session(stream);
    } else {
        let mut response = String::new();
        stream.read_to_string(&mut response).expect("Failed to read response");
        print!("{}", response);
    }
}

fn handle_interactive_session(mut stream: UnixStream) {
    let original_termios = setup_terminal_raw_mode();
    stream.set_nonblocking(true).expect("Failed to set non-blocking");
    let stream_clone = match stream.try_clone() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to clone stream: {}", e);
            restore_terminal_mode(&original_termios);
            return;
        }
    };
    
    let running = Arc::new(AtomicBool::new(true));
    let running_clone = running.clone();
    let stdin_to_stream = thread::spawn(move || {
        let mut stdin = io::stdin();
        let mut buffer = [0; 1024];
        let mut stream_write = stream_clone;
        while running.load(Ordering::SeqCst) {
            match stdin.read(&mut buffer) {
                Ok(0) => break,
                Ok(n) => {
                    if let Err(_) = stream_write.write_all(&buffer[..n]) {
                        break;
                    }
                    let _ = stream_write.flush();
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(e) => {
                    if e.kind() != io::ErrorKind::Interrupted {
                        break;
                    }
                }
            }
        }
    });

    let mut stdout = io::stdout();
    let mut buffer = [0; 1024];
    while running_clone.load(Ordering::SeqCst) {
        match stream.read(&mut buffer) {
            Ok(0) => {
                running_clone.store(false, Ordering::SeqCst);
                break;
            },
            Ok(n) => {
                if let Err(_) = stdout.write_all(&buffer[..n]) {
                    running_clone.store(false, Ordering::SeqCst);
                    break;
                }
                let _ = stdout.flush();
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(e) => {
                if e.kind() != io::ErrorKind::Interrupted {
                    running_clone.store(false, Ordering::SeqCst);
                    break;
                }
            }
        }
    }
    
    restore_terminal_mode(&original_termios);
    let _ = stdin_to_stream.join();
}

fn setup_terminal_raw_mode() -> nix::sys::termios::Termios {
    if !isatty(libc::STDIN_FILENO).unwrap_or(false) {
        return unsafe { std::mem::zeroed() };
    }

    let termios = nix::sys::termios::tcgetattr(libc::STDIN_FILENO).unwrap();
    let mut new_termios = termios.clone();
    new_termios.input_flags &= !(nix::sys::termios::InputFlags::BRKINT 
                                | nix::sys::termios::InputFlags::ICRNL 
                                | nix::sys::termios::InputFlags::INPCK 
                                | nix::sys::termios::InputFlags::ISTRIP 
                                | nix::sys::termios::InputFlags::IXON);
    
    new_termios.output_flags &= !(nix::sys::termios::OutputFlags::OPOST);
    new_termios.control_flags &= !(nix::sys::termios::ControlFlags::CSIZE 
                                 | nix::sys::termios::ControlFlags::PARENB);
    
    new_termios.control_flags |= nix::sys::termios::ControlFlags::CS8;
    new_termios.local_flags &= !(nix::sys::termios::LocalFlags::ECHO 
                               | nix::sys::termios::LocalFlags::ICANON 
                               | nix::sys::termios::LocalFlags::IEXTEN 
                               | nix::sys::termios::LocalFlags::ISIG);
    
    new_termios.control_chars[nix::sys::termios::SpecialCharacterIndices::VMIN as usize] = 1;
    new_termios.control_chars[nix::sys::termios::SpecialCharacterIndices::VTIME as usize] = 0;
    nix::sys::termios::tcsetattr(
        libc::STDIN_FILENO,
        nix::sys::termios::SetArg::TCSANOW,
        &new_termios,
    ).unwrap();
    let _ = unsafe { libc::fcntl(libc::STDIN_FILENO, libc::F_SETFL, libc::O_NONBLOCK) };
    install_signal_handlers();
    termios
}

fn restore_terminal_mode(termios: &nix::sys::termios::Termios) {
    if isatty(libc::STDIN_FILENO).unwrap_or(false) {
        let _ = nix::sys::termios::tcsetattr(
            libc::STDIN_FILENO,
            nix::sys::termios::SetArg::TCSANOW,
            termios,
        );
    }
}

fn install_signal_handlers() {
    unsafe {
        let mut act: libc::sigaction = std::mem::zeroed();
        act.sa_sigaction = handle_signal as usize;
        act.sa_flags = libc::SA_SIGINFO;
        libc::sigemptyset(&mut act.sa_mask as *mut libc::sigset_t);
        libc::sigaction(libc::SIGWINCH, &act, std::ptr::null_mut());
    }
}

extern "C" fn handle_signal(_signum: libc::c_int, _info: *mut libc::siginfo_t, _context: *mut libc::c_void) {
    unsafe {
        let mut ws: libc::winsize = std::mem::zeroed();
        if libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, &mut ws as *mut libc::winsize) != -1 {
            // todo, ig
        }
    }
}

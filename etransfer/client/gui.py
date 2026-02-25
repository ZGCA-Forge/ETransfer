"""Tkinter GUI for ETransfer client."""

import queue
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Optional

from etransfer import __version__

BOTH = "both"
X = "x"
Y = "y"
LEFT = "left"
RIGHT = "right"
TOP = "top"
BOTTOM = "bottom"
W = "w"
E = "e"
N = "n"
S = "s"
EW = "ew"
NS = "ns"
NSEW = "nsew"
NORMAL = "normal"
DISABLED = "disabled"
VERTICAL = "vertical"
HORIZONTAL = "horizontal"
SUNKEN = "sunken"
END = "end"


def format_size(size: int) -> str:
    """Format size in human-readable format."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024  # type: ignore[assignment]
    return f"{size:.1f} PB"


def format_rate(rate: float) -> str:
    """Format transfer rate."""
    return f"{format_size(int(rate))}/s"


class EasyTransferGUI:
    """Main GUI application for EasyTransfer."""

    def __init__(self, root: "tk.Tk") -> None:
        """Initialize GUI.

        Args:
            root: Tkinter root window
        """
        self.root = root
        self.root.title("ETransfer")
        self.root.geometry("800x600")
        self.root.minsize(600, 400)

        # State
        self.server_url = tk.StringVar(value="http://localhost:8765")
        self.token = tk.StringVar(value="")
        self.current_file: Optional[Path] = None
        self.is_uploading = False
        self.is_downloading = False

        # Message queue for thread communication
        self.msg_queue: queue.Queue[tuple[str, object]] = queue.Queue()

        # Build UI
        self._create_menu()
        self._create_main_frame()

        # Start message processing
        self._process_messages()

    def _create_menu(self) -> None:
        """Create menu bar."""
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)

        # File menu
        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="File", menu=file_menu)
        file_menu.add_command(label="Upload File...", command=self._select_upload_file)
        file_menu.add_command(label="Download File...", command=self._show_download_dialog)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.root.quit)

        # Help menu
        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Help", menu=help_menu)
        help_menu.add_command(label="About", command=self._show_about)

    def _create_main_frame(self) -> None:
        """Create main content frame."""
        # Main container
        main_frame = ttk.Frame(self.root, padding=10)
        main_frame.pack(fill=BOTH, expand=True)

        # Server settings frame
        settings_frame = ttk.LabelFrame(main_frame, text="Server Settings", padding=10)
        settings_frame.pack(fill=X, pady=(0, 10))

        ttk.Label(settings_frame, text="Server URL:").grid(row=0, column=0, sticky=W, padx=5)
        server_entry = ttk.Entry(settings_frame, textvariable=self.server_url, width=40)
        server_entry.grid(row=0, column=1, sticky=EW, padx=5)

        ttk.Label(settings_frame, text="API Token:").grid(row=1, column=0, sticky=W, padx=5, pady=5)
        token_entry = ttk.Entry(settings_frame, textvariable=self.token, width=40, show="*")
        token_entry.grid(row=1, column=1, sticky=EW, padx=5, pady=5)

        connect_btn = ttk.Button(settings_frame, text="Connect", command=self._connect_server)
        connect_btn.grid(row=0, column=2, rowspan=2, padx=10)

        settings_frame.columnconfigure(1, weight=1)

        # Notebook for tabs
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.pack(fill=BOTH, expand=True)
        notebook = self.notebook

        # Upload tab
        upload_frame = ttk.Frame(notebook, padding=10)
        notebook.add(upload_frame, text="Upload")
        self._create_upload_tab(upload_frame)

        # Download tab
        download_frame = ttk.Frame(notebook, padding=10)
        notebook.add(download_frame, text="Download")
        self._create_download_tab(download_frame)

        # Files tab
        files_frame = ttk.Frame(notebook, padding=10)
        notebook.add(files_frame, text="Files")
        self._create_files_tab(files_frame)

        # Server Info tab
        info_frame = ttk.Frame(notebook, padding=10)
        notebook.add(info_frame, text="Server Info")
        self._create_info_tab(info_frame)

        # Status bar
        self.status_var = tk.StringVar(value="Ready")
        status_bar = ttk.Label(main_frame, textvariable=self.status_var, relief=SUNKEN)
        status_bar.pack(fill=X, pady=(10, 0))

    def _create_upload_tab(self, parent: ttk.Frame) -> None:
        """Create upload tab content."""
        # File selection
        file_frame = ttk.Frame(parent)
        file_frame.pack(fill=X, pady=5)

        ttk.Label(file_frame, text="File:").pack(side=LEFT)
        self.upload_file_var = tk.StringVar()
        file_entry = ttk.Entry(file_frame, textvariable=self.upload_file_var, state="readonly")
        file_entry.pack(side=LEFT, fill=X, expand=True, padx=5)

        browse_btn = ttk.Button(file_frame, text="Browse...", command=self._select_upload_file)
        browse_btn.pack(side=LEFT)

        # Progress
        progress_frame = ttk.Frame(parent)
        progress_frame.pack(fill=X, pady=10)

        self.upload_progress_var = tk.DoubleVar(value=0)
        self.upload_progress = ttk.Progressbar(progress_frame, variable=self.upload_progress_var, maximum=100)
        self.upload_progress.pack(fill=X)

        self.upload_status_var = tk.StringVar(value="Select a file to upload")
        ttk.Label(progress_frame, textvariable=self.upload_status_var).pack(pady=5)

        # Upload button
        self.upload_btn = ttk.Button(parent, text="Upload", command=self._start_upload, state=DISABLED)
        self.upload_btn.pack(pady=10)

    def _create_download_tab(self, parent: ttk.Frame) -> None:
        """Create download tab content."""
        # File ID input
        id_frame = ttk.Frame(parent)
        id_frame.pack(fill=X, pady=5)

        ttk.Label(id_frame, text="File ID:").pack(side=LEFT)
        self.download_file_id_var = tk.StringVar()
        id_entry = ttk.Entry(id_frame, textvariable=self.download_file_id_var)
        id_entry.pack(side=LEFT, fill=X, expand=True, padx=5)

        # Output directory
        output_frame = ttk.Frame(parent)
        output_frame.pack(fill=X, pady=5)

        ttk.Label(output_frame, text="Save to:").pack(side=LEFT)
        self.download_output_var = tk.StringVar(value=str(Path.home() / "Downloads"))
        output_entry = ttk.Entry(output_frame, textvariable=self.download_output_var)
        output_entry.pack(side=LEFT, fill=X, expand=True, padx=5)

        browse_btn = ttk.Button(output_frame, text="Browse...", command=self._select_download_dir)
        browse_btn.pack(side=LEFT)

        # Progress
        progress_frame = ttk.Frame(parent)
        progress_frame.pack(fill=X, pady=10)

        self.download_progress_var = tk.DoubleVar(value=0)
        self.download_progress = ttk.Progressbar(progress_frame, variable=self.download_progress_var, maximum=100)
        self.download_progress.pack(fill=X)

        self.download_status_var = tk.StringVar(value="Enter a file ID to download")
        ttk.Label(progress_frame, textvariable=self.download_status_var).pack(pady=5)

        # Download button
        self.download_btn = ttk.Button(parent, text="Download", command=self._start_download)
        self.download_btn.pack(pady=10)

    def _create_files_tab(self, parent: ttk.Frame) -> None:
        """Create files list tab content."""
        # Treeview for files
        columns = ("filename", "size", "progress", "status")
        self.files_tree = ttk.Treeview(parent, columns=columns, show="headings")

        self.files_tree.heading("filename", text="Filename")
        self.files_tree.heading("size", text="Size")
        self.files_tree.heading("progress", text="Progress")
        self.files_tree.heading("status", text="Status")

        self.files_tree.column("filename", width=300)
        self.files_tree.column("size", width=100)
        self.files_tree.column("progress", width=100)
        self.files_tree.column("status", width=100)

        scrollbar = ttk.Scrollbar(parent, orient=VERTICAL, command=self.files_tree.yview)
        self.files_tree.configure(yscrollcommand=scrollbar.set)

        self.files_tree.pack(side=LEFT, fill=BOTH, expand=True)
        scrollbar.pack(side=RIGHT, fill=Y)

        # Buttons
        btn_frame = ttk.Frame(parent)
        btn_frame.pack(fill=X, pady=10)

        refresh_btn = ttk.Button(btn_frame, text="Refresh", command=self._refresh_files)
        refresh_btn.pack(side=LEFT, padx=5)

        download_btn = ttk.Button(btn_frame, text="Download Selected", command=self._download_selected)
        download_btn.pack(side=LEFT, padx=5)

    def _create_info_tab(self, parent: ttk.Frame) -> None:
        """Create server info tab content."""
        # Info display
        self.info_text = tk.Text(parent, height=20, state=DISABLED)
        self.info_text.pack(fill=BOTH, expand=True)

        refresh_btn = ttk.Button(parent, text="Refresh", command=self._refresh_server_info)
        refresh_btn.pack(pady=10)

    def _select_upload_file(self) -> None:
        """Open file dialog to select file for upload."""
        file_path = filedialog.askopenfilename(title="Select File to Upload")
        if file_path:
            self.current_file = Path(file_path)
            self.upload_file_var.set(file_path)
            self.upload_btn.config(state=NORMAL)
            file_size = self.current_file.stat().st_size
            self.upload_status_var.set(f"Ready to upload: {format_size(file_size)}")

    def _select_download_dir(self) -> None:
        """Open directory dialog to select download location."""
        dir_path = filedialog.askdirectory(title="Select Download Directory")
        if dir_path:
            self.download_output_var.set(dir_path)

    def _show_download_dialog(self) -> None:
        """Switch to the download tab."""
        self.notebook.select(1)

    def _connect_server(self) -> None:
        """Test connection to server."""
        self.status_var.set("Connecting...")

        def connect() -> None:
            try:
                from etransfer.client.server_info import check_server_health

                is_healthy = check_server_health(
                    self.server_url.get(),
                    token=self.token.get() or None,
                )

                if is_healthy:
                    self.msg_queue.put(("status", "Connected to server"))
                    self.msg_queue.put(("info", "Connection successful!"))
                else:
                    self.msg_queue.put(("status", "Connection failed"))
                    self.msg_queue.put(("error", "Server is not responding"))
            except Exception as e:
                self.msg_queue.put(("status", "Connection error"))
                self.msg_queue.put(("error", str(e)))

        threading.Thread(target=connect, daemon=True).start()

    def _start_upload(self) -> None:
        """Start file upload in background thread."""
        if not self.current_file or self.is_uploading:
            return

        self.is_uploading = True
        self.upload_btn.config(state=DISABLED)
        self.upload_progress_var.set(0)

        def upload() -> None:
            try:
                from etransfer.client.tus_client import EasyTransferClient

                def progress_callback(uploaded: int, total: int) -> None:
                    progress = (uploaded / total) * 100
                    self.msg_queue.put(("upload_progress", progress))
                    self.msg_queue.put(("upload_status", f"Uploading: {format_size(uploaded)} / {format_size(total)}"))

                with EasyTransferClient(
                    self.server_url.get(),
                    token=self.token.get() or None,
                ) as client:
                    uploader = client.create_uploader(
                        str(self.current_file),
                        progress_callback=progress_callback,
                    )
                    uploader.upload()

                self.msg_queue.put(("upload_progress", 100))
                self.msg_queue.put(("upload_status", "Upload complete!"))
                self.msg_queue.put(("upload_done", True))
                self.msg_queue.put(("info", "File uploaded successfully!"))

            except Exception as e:
                self.msg_queue.put(("upload_status", f"Error: {e}"))
                self.msg_queue.put(("upload_done", False))
                self.msg_queue.put(("error", str(e)))

        threading.Thread(target=upload, daemon=True).start()

    def _start_download(self) -> None:
        """Start file download in background thread."""
        file_id = self.download_file_id_var.get().strip()
        if not file_id or self.is_downloading:
            return

        self.is_downloading = True
        self.download_btn.config(state=DISABLED)
        self.download_progress_var.set(0)

        def download() -> None:
            try:
                from etransfer.client.downloader import ChunkDownloader

                downloader = ChunkDownloader(
                    self.server_url.get(),
                    token=self.token.get() or None,
                )

                # Get file info
                info = downloader.get_file_info(file_id)
                output_dir = Path(self.download_output_var.get())
                output_path = output_dir / info.filename

                def progress_callback(downloaded: int, total: int) -> None:
                    progress = (downloaded / total) * 100
                    self.msg_queue.put(("download_progress", progress))
                    self.msg_queue.put(
                        ("download_status", f"Downloading: {format_size(downloaded)} / {format_size(total)}")
                    )

                success = downloader.download_file(
                    file_id,
                    output_path,
                    progress_callback=progress_callback,
                )

                if success:
                    self.msg_queue.put(("download_progress", 100))
                    self.msg_queue.put(("download_status", f"Downloaded to: {output_path}"))
                    self.msg_queue.put(("download_done", True))
                    self.msg_queue.put(("info", f"File downloaded to: {output_path}"))
                else:
                    self.msg_queue.put(("download_status", "Download failed"))
                    self.msg_queue.put(("download_done", False))
                    self.msg_queue.put(("error", "Download failed"))

            except Exception as e:
                self.msg_queue.put(("download_status", f"Error: {e}"))
                self.msg_queue.put(("download_done", False))
                self.msg_queue.put(("error", str(e)))

        threading.Thread(target=download, daemon=True).start()

    def _refresh_files(self) -> None:
        """Refresh file list from server."""
        self.status_var.set("Loading files...")

        def load_files() -> None:
            try:
                from etransfer.client.tus_client import EasyTransferClient

                with EasyTransferClient(
                    self.server_url.get(),
                    token=self.token.get() or None,
                ) as client:
                    files = client.list_files()

                self.msg_queue.put(("files", files))
                self.msg_queue.put(("status", f"Loaded {len(files)} files"))

            except Exception as e:
                self.msg_queue.put(("status", "Error loading files"))
                self.msg_queue.put(("error", str(e)))

        threading.Thread(target=load_files, daemon=True).start()

    def _download_selected(self) -> None:
        """Download selected file from list."""
        selection = self.files_tree.selection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a file to download")
            return

        # Get file ID from selection
        item = self.files_tree.item(selection[0])
        file_id = item.get("tags", [None])[0]
        if file_id:
            self.download_file_id_var.set(file_id)
            # Switch to download tab and start
            self._start_download()

    def _refresh_server_info(self) -> None:
        """Refresh server information."""
        self.status_var.set("Loading server info...")

        def load_info() -> None:
            try:
                from etransfer.client.tus_client import EasyTransferClient

                with EasyTransferClient(
                    self.server_url.get(),
                    token=self.token.get() or None,
                ) as client:
                    info = client.get_server_info()

                self.msg_queue.put(("server_info", info))
                self.msg_queue.put(("status", "Server info loaded"))

            except Exception as e:
                self.msg_queue.put(("status", "Error loading server info"))
                self.msg_queue.put(("error", str(e)))

        threading.Thread(target=load_info, daemon=True).start()

    def _show_about(self) -> None:
        """Show about dialog."""
        messagebox.showinfo(
            "About ETransfer",
            f"ETransfer\nVersion {__version__}\n\nA TUS-based file transfer tool",
        )

    def _process_messages(self) -> None:
        """Process messages from background threads."""
        try:
            while True:
                msg_type, msg_data = self.msg_queue.get_nowait()

                if msg_type == "status":
                    self.status_var.set(msg_data)

                elif msg_type == "error":
                    messagebox.showerror("Error", msg_data)

                elif msg_type == "info":
                    messagebox.showinfo("Info", msg_data)

                elif msg_type == "upload_progress":
                    self.upload_progress_var.set(msg_data)

                elif msg_type == "upload_status":
                    self.upload_status_var.set(msg_data)

                elif msg_type == "upload_done":
                    self.is_uploading = False
                    self.upload_btn.config(state=NORMAL)

                elif msg_type == "download_progress":
                    self.download_progress_var.set(msg_data)

                elif msg_type == "download_status":
                    self.download_status_var.set(msg_data)

                elif msg_type == "download_done":
                    self.is_downloading = False
                    self.download_btn.config(state=NORMAL)

                elif msg_type == "files":
                    # Clear existing items
                    for item in self.files_tree.get_children():
                        self.files_tree.delete(item)

                    # Add new items
                    for f in msg_data:
                        self.files_tree.insert(
                            "",
                            END,
                            values=(
                                f.filename,
                                format_size(f.size),
                                f"{f.progress:.1f}%",
                                f.status.value,
                            ),
                            tags=(f.file_id,),
                        )

                elif msg_type == "server_info":
                    info = msg_data
                    self.info_text.config(state=NORMAL)
                    self.info_text.delete(1.0, END)

                    text = f"""Server Information
================

Version: {info.version}
TUS Version: {info.tus_version}
Chunk Size: {format_size(info.chunk_size)}
Total Files: {info.total_files}
Total Storage: {format_size(info.total_size)}

Endpoints:
"""
                    for ep in info.endpoints:
                        text += f"""
  {ep.endpoint}:
    Upload: {format_rate(ep.upload_rate)}
    Download: {format_rate(ep.download_rate)}
"""

                    self.info_text.insert(1.0, text)
                    self.info_text.config(state=DISABLED)

        except queue.Empty:
            pass

        # Schedule next check
        self.root.after(100, self._process_messages)


def run_gui() -> None:
    """Run the GUI application."""
    root = tk.Tk()
    _ = EasyTransferGUI(root)
    root.mainloop()


if __name__ == "__main__":
    run_gui()

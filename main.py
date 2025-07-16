import tkinter as tk
from downloader import *
from tkinter import messagebox

def _main():
    root = tk.Tk()
    root.geometry("800x600+0+0")
    root.title("Multi Threading Downloader")

    url_label = tk.Label(text="url: ")
    url_entry = tk.Entry(root)
    output_label = tk.Label(text="output: ")
    output_entry = tk.Entry(root)
    threads_num_label = tk.Label(text="Threads num: ")
    threads_num_entry = tk.Entry(root)

    def download():
        sucess =download_file(
            url=url_entry.get(),
            output_path=output_entry.get(),
            num_threads=int(threads_num_entry.get()), 
            resume=False, 
            expected_hash=None,
            hash_algorithm="md5"
            )
        if sucess:
            messagebox.showinfo("Download sucess","Download sucess")
        else:
            messagebox.showerror("Download error","Download error")

    download_button = tk.Button(text="Download", command=download)

    url_label.pack()
    url_entry.pack()
    output_label.pack()
    output_entry.pack()
    threads_num_label.pack()
    threads_num_entry.pack()

    download_button.pack()

    root.mainloop()

if __name__ == "__main__":
    _main()
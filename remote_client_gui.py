#!/usr/bin/env python3
from __future__ import annotations

import json
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox
from typing import List, Optional, Tuple

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from parcel_crawl_demo_v4 import FootprintProfile, normalize_vector, prepare_footprint, prompt_front_direction

API_BASE_DEFAULT = "https://landlens.up.railway.app"


def polygon_to_points(profile: FootprintProfile) -> List[List[float]]:
    coords = list(profile.geometry.exterior.coords)
    if coords and coords[0] == coords[-1]:
        coords = coords[:-1]
    return [[float(x), float(y)] for x, y in coords]


class RemoteClientApp:
    def __init__(self) -> None:
        self.root = tk.Tk()
        self.root.title("LandLens Remote Crawl")
        self.root.geometry("560x460")

        self.api_var = tk.StringVar(value=API_BASE_DEFAULT)
        self.address_var = tk.StringVar()
        self.cycles_var = tk.StringVar(value="3")
        self.buffer_var = tk.StringVar(value="80")
        self.rotation_var = tk.StringVar(value="15")
        self.score_workers_var = tk.StringVar(value="4")

        self.dxf_path: Optional[Path] = None
        self.footprint_points: Optional[List[List[float]]] = None
        self.front_vector: Optional[Tuple[float, float]] = None
        self.remote_files: List[dict] = []
        self.selected_remote: Optional[dict] = None

        self.status_var = tk.StringVar(value="Select a DXF to begin.")
        self.result_box = tk.Text(self.root, height=10, state="disabled")

        self._build_form()

    def _build_form(self) -> None:
        frame = tk.Frame(self.root)
        frame.pack(fill="x", padx=12, pady=8)

        tk.Label(frame, text="API Base URL").grid(row=0, column=0, sticky="w")
        tk.Entry(frame, textvariable=self.api_var, width=50).grid(row=0, column=1, sticky="we")

        tk.Label(frame, text="Address").grid(row=1, column=0, sticky="w", pady=(6, 0))
        tk.Entry(frame, textvariable=self.address_var, width=50).grid(row=1, column=1, sticky="we", pady=(6, 0))

        tk.Label(frame, text="Cycles").grid(row=2, column=0, sticky="w", pady=(6, 0))
        tk.Entry(frame, textvariable=self.cycles_var, width=10).grid(row=2, column=1, sticky="w", pady=(6, 0))

        tk.Label(frame, text="Buffer (m)").grid(row=3, column=0, sticky="w", pady=(6, 0))
        tk.Entry(frame, textvariable=self.buffer_var, width=10).grid(row=3, column=1, sticky="w", pady=(6, 0))

        tk.Label(frame, text="Rotation step (deg)").grid(row=4, column=0, sticky="w", pady=(6, 0))
        tk.Entry(frame, textvariable=self.rotation_var, width=10).grid(row=4, column=1, sticky="w", pady=(6, 0))

        tk.Label(frame, text="Score Workers").grid(row=5, column=0, sticky="w", pady=(6, 0))
        tk.Entry(frame, textvariable=self.score_workers_var, width=10).grid(row=5, column=1, sticky="w", pady=(6, 0))

        btn_frame = tk.Frame(self.root)
        btn_frame.pack(fill="x", padx=12, pady=8)

        tk.Button(btn_frame, text="Select DXF", command=self.pick_dxf).pack(side="left")
        tk.Button(btn_frame, text="Capture Footprint", command=self.capture_footprint).pack(side="left", padx=6)
        tk.Button(btn_frame, text="Upload DXF", command=self.upload_file).pack(side="left")
        tk.Button(btn_frame, text="Start Crawl", command=self.start_crawl).pack(side="left", padx=6)

        files_frame = tk.LabelFrame(self.root, text="Remote Files")
        files_frame.pack(fill="both", expand=False, padx=12, pady=(0, 8))
        self.file_list = tk.Listbox(files_frame, height=6)
        self.file_list.pack(fill="both", expand=True, side="left", padx=(0, 8))
        self.file_list.bind("<<ListboxSelect>>", self.on_select_remote)
        tk.Button(files_frame, text="Refresh", command=self.refresh_files).pack(side="left")

        self.result_box.pack(fill="both", expand=True, padx=12, pady=8)
        tk.Label(self.root, textvariable=self.status_var, anchor="w").pack(fill="x", padx=12, pady=(0, 8))

        self.refresh_files()

    def pick_dxf(self) -> None:
        path = filedialog.askopenfilename(title="Select DXF", filetypes=[("DXF files", "*.dxf"), ("All files", "*.*")])
        if path:
            self.dxf_path = Path(path).expanduser().resolve()
            self.status_var.set(f"Selected {self.dxf_path.name}")
            self.footprint_points = None
            self.front_vector = None

    def capture_footprint(self) -> None:
        if not self.dxf_path:
            messagebox.showerror("Missing DXF", "Please select a DXF file first.", parent=self.root)
            return
        self.status_var.set("Capturing footprint…")
        self.root.update_idletasks()
        try:
            profile, front = prepare_footprint(self.dxf_path)
            if front is None:
                front = prompt_front_direction(profile.geometry)
            front = normalize_vector(front)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Capture failed", str(exc), parent=self.root)
            self.status_var.set("Capture failed.")
            return

        self.footprint_points = polygon_to_points(profile)
        self.front_vector = front
        self.status_var.set(f"Footprint ready ({len(self.footprint_points)} points).")
        self._write_result({
            "footprint_points": self.footprint_points,
            "front_vector": [round(front[0], 4), round(front[1], 4)],
        })

    def upload_file(self) -> None:
        if not self.dxf_path:
            messagebox.showerror("Missing DXF", "Select a DXF file first.", parent=self.root)
            return
        self.status_var.set("Uploading DXF…")
        self.root.update_idletasks()
        try:
            payload = self._upload_file()
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Upload failed", str(exc), parent=self.root)
            self.status_var.set("Upload failed.")
            return
        self.status_var.set(f"Uploaded {payload['filename']}")
        self.refresh_files()
        self.selected_remote = payload

    def start_crawl(self) -> None:
        if not self.selected_remote:
            messagebox.showerror("Missing file", "Select or upload a DXF to use.", parent=self.root)
            return
        if not self.footprint_points or not self.front_vector:
            messagebox.showerror("Missing footprint", "Capture the footprint/frontage before uploading.", parent=self.root)
            return
        address = self.address_var.get().strip()
        if not address:
            messagebox.showerror("Missing address", "Enter an address for the crawl.", parent=self.root)
            return
        try:
            job = self._start_job(address, self.selected_remote)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Job failed", str(exc), parent=self.root)
            return
        self.status_var.set(f"Job {job['id']} queued.")
        self._write_result(job)
        self.root.after(3000, lambda: self.poll_job(job["id"]))

    def _upload_file(self) -> dict:
        url = self.api_var.get().rstrip("/") + "/files"
        with self.dxf_path.open("rb") as handle:  # type: ignore[union-attr]
            resp = requests.post(url, files={"file": (self.dxf_path.name, handle)}, timeout=900, verify=False)
        resp.raise_for_status()
        return resp.json()

    def _start_job(self, address: str, upload_payload: dict) -> dict:
        url = self.api_var.get().rstrip("/") + "/jobs"
        config = {
            "cycles": int(self.cycles_var.get() or 1),
            "buffer": float(self.buffer_var.get() or 80),
            "rotation_step": float(self.rotation_var.get() or 15),
            "score_workers": int(self.score_workers_var.get() or 1),
        }
        job_payload = {
            "address": address,
            "dxf_url": upload_payload["file_url"],
            "config": config,
            "footprint_points": self.footprint_points,
            "front_direction": [self.front_vector[0], self.front_vector[1]],
        }
        resp = requests.post(url, json=job_payload, timeout=60, verify=False)
        resp.raise_for_status()
        return resp.json()

    def poll_job(self, job_id: str) -> None:
        url = self.api_var.get().rstrip("/") + f"/jobs/{job_id}"
        try:
            resp = requests.get(url, timeout=30, verify=False)
            resp.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            self.status_var.set(f"Job {job_id}: poll failed ({exc})")
            return
        payload = resp.json()
        self._write_result(payload)
        status = payload.get("status")
        if status in {"queued", "running"}:
            self.status_var.set(f"Job {job_id}: {status}")
            self.root.after(4000, lambda: self.poll_job(job_id))
        else:
            self.status_var.set(f"Job {job_id}: {status}")

    def refresh_files(self) -> None:
        url = self.api_var.get().rstrip("/") + "/files"
        try:
            resp = requests.get(url, timeout=30, verify=False)
            resp.raise_for_status()
            self.remote_files = resp.json()
        except Exception as exc:  # noqa: BLE001
            self.status_var.set(f"Failed to fetch files: {exc}")
            self.remote_files = []
        self.file_list.delete(0, tk.END)
        for item in self.remote_files:
            self.file_list.insert(tk.END, item["filename"])
        self.selected_remote = None

    def on_select_remote(self, _event: object) -> None:
        if not self.file_list.curselection():
            return
        idx = self.file_list.curselection()[0]
        if idx < len(self.remote_files):
            self.selected_remote = self.remote_files[idx]
            self.status_var.set(f"Selected remote file {self.selected_remote['filename']}")

    def _write_result(self, payload: dict) -> None:
        self.result_box.configure(state="normal")
        self.result_box.delete("1.0", tk.END)
        self.result_box.insert(tk.END, json.dumps(payload, indent=2))
        self.result_box.configure(state="disabled")

    def run(self) -> None:
        self.root.mainloop()


if __name__ == "__main__":
    RemoteClientApp().run()

(*
 * Copyright (C) 2011 Citrix Systems Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation; version 2.1 only. with the special
 * exception on linking described in file LICENSE.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *)

open Xenops_utils

module D = Debug.Make(struct let name = "xenops" end)
open D
open Xenstore
open Xenops_task

module Uuid = Uuidm

let fs_backend_path = "/usr/sbin/fs-backend"

let memory_mib = 32L
let memory_kib = Int64.mul 1024L memory_mib

let create ~xc ~xs domid =
  let stubdom_name = Printf.sprintf "stubdom:%d" domid in
  let stubdom_uuid = Uuid.create `V4 in
  debug "creating stubdom with name '%s' and uuid '%s' (%s)"
    stubdom_name (Uuid.to_string stubdom_uuid) __LOC__;
  let info = {
    Domain.ssidref = 0l;
    Domain.hvm = false;
    Domain.hap = false;
    Domain.name = stubdom_name;
    Domain.platformdata = [];
    Domain.xsdata = [];
    Domain.bios_strings = [];
    Domain.has_vendor_device = false;
    Domain.is_uefi = false;
  } in
  let stubdom_domid = Domain.make ~xc ~xs info Domain.(X86 { emulation_flags = [] }) stubdom_uuid in
  debug "jjd27: created stubdom with domid %d" stubdom_domid;

  Domain.set_machine_address_size ~xc stubdom_domid (Some 32);
  stubdom_domid

let build (task: Xenops_task.task_handle) ~xc ~xs ~dm ~store_domid ~console_domid info xenguest domid stubdom_domid =
  (* Now build it as a PV domain *)
  let () = Domain.build task ~xc ~xs ~store_domid ~console_domid ~timeoffset:"" ~extras:[] ~vgpus:[] {
      Domain.memory_max=memory_kib;
      Domain.memory_target=memory_kib;
      Domain.kernel="/usr/lib/xen/boot/ioemu-stubdom.gz";
      Domain.vcpus=1;
      Domain.priv=Domain.BuildPV {Domain.cmdline=""; Domain.ramdisk=None};
    } xenguest stubdom_domid false in

  (* Point the stub domain at the guest *)
  debug "jjd27: pointing stubdom %d to guest %d" stubdom_domid domid;
  Xenctrlext.domain_set_target xc stubdom_domid domid;

  (* Tell XenStore that the stubdom should have implicit privileges over the target domain *)
  debug "jjd27: telling XenStore that stubdom %d has target %d" stubdom_domid domid;
  xs.Xs.set_target stubdom_domid domid;

  (* Write the guest's domid into XenStore *)
  let path = xs.Xs.getdomainpath stubdom_domid in
  xs.Xs.write (Printf.sprintf "%s/target" path) (string_of_int domid);
  debug "jjd27: writing guest domid %d into xenstore at %s/target" domid path;

  let vm = xs.Xs.read (Printf.sprintf "/local/domain/%d/vm" domid) in
  debug "jjd27: target vm's path is %s\n" vm;

  (* Write the qemu-dm command-line arguments into XenStore *)
  let guest_uuid_str = Uuid.to_string (Domain.get_uuid xc domid) in
  let path = Printf.sprintf "/vm/%s/image/dmargs" guest_uuid_str in
  (* Remove any 'pty' references from the arguments: XXX why? *)
  let info' = { info with Device.Dm.serial = None; monitor = None } in
  let args = Device.Dm.qemu_args ~xs ~dm info' false domid in
  let argv = args.Device.Dm.argv in
  xs.Xs.write path (String.concat " " argv);
  debug "jjd27: written qemu-dm args into xenstore at %s: [%s]" path (String.concat " " argv);

  (* Make that XenStore path readable by the stub domain *)
  xs.Xs.setperms path Xs_protocol.ACL.({owner = 0; other = NONE; acl = [ (domid, READ); (stubdom_domid, READ) ]});
  debug "jjd27: set the permissions on %s" path;

  let syslog_stdout = Forkhelpers.Syslog_WithKey (Printf.sprintf "fs-backend-%d" domid) in
  let pid = Forkhelpers.safe_close_and_exec None None None [] ~syslog_stdout fs_backend_path [] in
  Forkhelpers.dontwaitpid pid;

  (* Set the FS backend in XenStore for the stubdom to have access to the domain filesystem *)
  Device.Vfs.add ~xc ~xs stubdom_domid;

  (* VFB is needed so we can see the framebuffer via the stubdom *)
  Device.Vfb.add ~xc ~xs stubdom_domid;

  (* VKBD is needed for keyboard input via the stubdom *)
  Device.Vkbd.add ~xc ~xs stubdom_domid;

  (* XXX:
     (* Add a place for qemu to record the dm state in XenStore, with appropriate permissions *)
     List.iter (fun domid -> Device.Dm.init ~xs ~domid) [stubdom_domid; domid];
     	*)

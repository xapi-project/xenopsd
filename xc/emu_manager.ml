(*
 * Copyright (C) 2006-2009 Citrix Systems Inc.
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
open Xenops_task

module D = Debug.Make(struct let name = "emu-manager" end)
open D

(* Exceptions which may propagate from the emu-manager binary *)
exception Xenctrl_dom_allocate_failure of int * string
exception Xenctrl_dom_linux_build_failure of int * string
exception Xenctrl_domain_save_failure of int * string
exception Xenctrl_domain_resume_failure of int * string
exception Xenctrl_domain_restore_failure of int * string

exception Domain_builder_error of string (* function name *) * int (* error code *) * string (* message *)

(** We do all our IO through the buffered channels but pass the
    underlying fds as integers to the forked helper on the commandline. *)
type t = in_channel * out_channel * Unix.file_descr * Unix.file_descr * Forkhelpers.pidty

(** Fork and run a emu-manager with particular args, leaving 'fds' open
    (in addition to internal control I/O fds) *)
let connect path domid (args: string list) (fds: (string * Unix.file_descr) list) : t =
  debug "connect: args = [ %s ]" (String.concat " " args);
  (* Need to send commands and receive responses from the
     	   slave process *)

  let slave_to_server_w_uuid = Uuidm.to_string (Uuidm.create `V4) in
  let server_to_slave_r_uuid = Uuidm.to_string (Uuidm.create `V4) in

  let slave_to_server_r, slave_to_server_w = Unix.pipe () in
  let server_to_slave_r, server_to_slave_w = Unix.pipe () in

  let args = [ "-controloutfd"; slave_to_server_w_uuid;
               "-controlinfd"; server_to_slave_r_uuid
             ] @ args in
  let pid = Forkhelpers.safe_close_and_exec None None None
      ([ slave_to_server_w_uuid, slave_to_server_w;
         server_to_slave_r_uuid, server_to_slave_r ] @ fds)
      path args in

  Unix.close slave_to_server_w;
  Unix.close server_to_slave_r;

  Unix.in_channel_of_descr slave_to_server_r,
  Unix.out_channel_of_descr server_to_slave_w,
  slave_to_server_r,
  server_to_slave_w,
  pid

(** Wait for the (hopefully dead) child process *)
let disconnect (_, _, r, w, pid) =
  Unix.close r;
  Unix.close w;
  ignore(Forkhelpers.waitpid pid)

type emu = Xenguest | Vgpu

let emu_of_string = function
  | "xenguest" -> Xenguest
  | "vgpu" -> Vgpu
  | _ -> failwith "unknown emu"

let string_of_emu = function
  | Xenguest -> "xenguest"
  | Vgpu -> "vgpu"

(** immediately write a command to the control channel *)
let send (_, out, _, _, _) txt = output_string out txt; flush out

let send_done cnx =
  send cnx "done\n"

let send_restore cnx emu =
  send cnx (Printf.sprintf "restore:%s\n" (string_of_emu emu))

let send_abort cnx = send cnx "abort\n"

type message =
  | Stdout of string (* captured stdout from emu-manager *)
  | Stderr of string (* captured stderr from emu-manager *)
  | Error of string  (* an actual error that we detected *)
  | Suspend          (* request the caller suspends the domain *)
  | Info of string   (* some info that we want to send back *)
  | Result of string (* the result of the operation *)
  | Prepare of string (* request the caller to prepare for the next step *)

let string_of_message = function
  | Stdout x  -> "stdout:" ^ (String.escaped x)
  | Stderr x  -> "stderr:" ^ (String.escaped x)
  | Error x   -> "error:" ^ (String.escaped x)
  | Suspend   -> "suspend:"
  | Info x    -> "info:" ^ (String.escaped x)
  | Result x  -> "result:" ^ (String.escaped x)
  | Prepare x -> "prepare:" ^ (String.escaped x)

let message_of_string x =
  if not(String.contains x ':')
  then failwith (Printf.sprintf "Failed to parse message from emu-manager [%s]" x);
  let i = String.index x ':' in
  let prefix = String.sub x 0 i
  and suffix = String.sub x (i + 1) (String.length x - i - 1) in match prefix with
  | "stdout" -> Stdout suffix
  | "stderr" -> Stderr suffix
  | "error" -> Error suffix
  | "suspend" -> Suspend
  | "info" -> Info suffix
  | "result" -> Result suffix
  | "prepare" -> Prepare suffix
  | _ -> Error "uncaught exception"

type result =
  | Xenguest_result of (nativeint * nativeint)
  | Vgpu_result

let emu_of_result = function
  | Xenguest_result _ -> Xenguest
  | Vgpu_result -> Vgpu

let parse_result res =
  match Astring.String.cuts ~sep:" " res with
  | [emu; store; console] when emu_of_string emu = Xenguest ->
    Xenguest_result (Nativeint.of_string store, Nativeint.of_string console)
  | [emu]                 when emu_of_string emu = Vgpu ->
    Vgpu_result
  | _ -> failwith "Unknown result type"

(** return the next output line from the control channel *)
let receive (infd, _, _, _, _) = message_of_string (input_line infd)

(** return the next output line which is not a debug: line *)
let rec non_debug_receive ?(debug_callback=(fun s -> debug "%s" s)) cnx = match receive cnx with
  | Stdout x -> debug_callback x; non_debug_receive ~debug_callback cnx
  | Stderr x -> debug_callback x; non_debug_receive ~debug_callback cnx
  | Info x -> debug_callback x; non_debug_receive ~debug_callback cnx
  | x -> x (* Error or Result or Suspend *)

(* Dump memory statistics on failure *)
let non_debug_receive ?debug_callback cnx =
  let debug_memory () =
    Xenctrl.with_intf (fun xc ->
        let open Memory in
        let open Int64 in
        let open Xenctrl in
        let p = Xenctrl.physinfo xc in
        error "Memory F %Ld KiB S %Ld KiB T %Ld MiB"
          (p.free_pages |> of_nativeint |> kib_of_pages)
          (p.scrub_pages |> of_nativeint |> kib_of_pages)
          (p.total_pages |> of_nativeint |> mib_of_pages_free)
      ) in
  try
    match non_debug_receive ?debug_callback cnx with
    | Error y as x ->
      error "Received: %s" y;
      debug_memory (); x
    | x -> x
  with e ->
    debug_memory ();
    raise e

let with_connection (task: Xenops_task.task_handle) path domid (args: string list) (fds: (string * Unix.file_descr) list) f =
  let t = connect path domid args fds in
  let cancelled = ref false in
  let cancel_cb () =
    let _, _, _, _, pid = t in
    let pid = Forkhelpers.getpid pid in
    cancelled := true;
    info "Cancelling task %s; sending 'abort' to emu-manager pid: %d"
      (Xenops_task.id_of_handle task) pid;
    send_abort t in
  Xapi_stdext_pervasives.Pervasiveext.finally
    (fun () ->
       Xenops_task.with_cancel task cancel_cb
         (fun () ->
            try
              f t
            with e ->
              if !cancelled
              then Xenops_task.raise_cancelled task
              else raise e
         )
    ) (fun () -> disconnect t)

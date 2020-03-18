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
open Printf
open Xenops_utils
open Xenstore
open Xenops_helpers
open Xenops_task
open Device_common

module D = Debug.Make(struct let name = "xenops" end)
open D

type key =
  | Device of device
  | Domain of int
  | Qemu of int * int
  | Vgpu of int
  | Varstored of int
  | TestPath of string

let string_of = function
  | Device device -> Printf.sprintf "device %s" (Device_common.string_of_device device)
  | Domain domid -> Printf.sprintf "domid %d" domid
  | Qemu (backend, frontend) -> Printf.sprintf "qemu backend = %d; frontend = %d" backend frontend
  | Vgpu domid -> Printf.sprintf "domid %d" domid
  | Varstored domid -> Printf.sprintf "varstored %d" domid
  | TestPath x -> x

let root = "/xenops/tasks"

let path_of domid =
  Printf.sprintf "%s/%d" root domid

let cancel_path_of ~xs = function
  | Device x ->
    (* Device operations can be cancelled separately *)
    Printf.sprintf "%s/%s/%d/cancel" (path_of x.frontend.domid) (string_of_kind x.backend.kind) x.backend.devid
  | Domain domid ->
    Printf.sprintf "%s/cancel" (path_of domid)
  | Qemu (backend, frontend) ->
    (* Domain and qemu watches are considered to be domain-local *)
    Printf.sprintf "%s/device-model/cancel" (path_of frontend)
  | Vgpu domid ->
    Printf.sprintf "%s/vgpu/cancel" (path_of domid)
  | Varstored domid ->
    Printf.sprintf "%s/varstored/cancel" (path_of domid)
  | TestPath x -> x

let shutdown_path_of ~xs = function
  | Device x -> Printf.sprintf "%s/%s/%d/shutdown" (path_of x.frontend.domid) (string_of_kind x.backend.kind) x.backend.devid
  | Domain domid -> Printf.sprintf "%s/shutdown" (path_of domid)
  | Qemu (backend, _) ->
    (* We only need to cancel when the backend domain shuts down. It will
       		   break suspend if we cancel when the frontend shuts down. *)
    Printf.sprintf "%s/shutdown" (path_of backend)
  | Vgpu domid -> Printf.sprintf "%s/vgpu/shutdown" (path_of domid)
  | Varstored domid -> Printf.sprintf "%s/varstored/shutdown" (path_of domid)
  | TestPath x -> x

let cleanup_for_domain ~xs domid =
  try
    xs.Xs.rm (path_of domid)
  with _ ->
    warn "Failed to clean up xenstore cancellation paths for domain %d" domid

let watches_of ~xs key = [
  Watch.key_to_disappear (cancel_path_of ~xs key);
  Watch.value_to_become (shutdown_path_of ~xs key) ""
]

let cancel ~xs key =
  let path = cancel_path_of ~xs key in
  if try ignore(xs.Xs.read path); true with _ -> false then begin
    info "Cancelling operation on device: %s" (string_of key);
    xs.Xs.rm path
  end

let on_shutdown ~xs domid =
  let path = shutdown_path_of ~xs (Domain domid) in
  let domainpath = xs.Xs.getdomainpath domid in
  (* Only write if the guest domain still exists *)
  Xs.transaction xs
    (fun t ->
       let exists = try ignore(t.Xst.read domainpath); true with _ -> false in
       if exists
       then begin
         let control_path = Printf.sprintf "%s/control/shutdown" domainpath in
         let shutdown_in_progress = try t.Xst.read control_path <> "" with _ -> false in
         if shutdown_in_progress then t.Xst.rm control_path
         else t.Xst.write path ""
       end
       else info "Not cancelling watches associated with domid: %d- domain nolonger exists" domid
    )

let with_path ~xs key f =
  let path = cancel_path_of ~xs key in
  Xapi_stdext_pervasives.Pervasiveext.finally
    (fun () ->
       xs.Xs.write path "";
       f ()
    )
    (fun () ->
       try
         xs.Xs.rm path
       with _ ->
         debug "ignoring cancel request: operation has already terminated";
         (* This means a cancel happened just as we succeeded;
            				   it was too late and we ignore it. *)
         ()
    )

let cancellable_watch key good_watches error_watches (task: Xenops_task.task_handle) ~xs ~timeout () =
  with_path ~xs key
    (fun () ->
       Xenops_task.with_cancel task
         (fun () ->
            with_xs (fun xs -> cancel ~xs key)
         )
         (fun () ->
            let cancel_watches = watches_of ~xs key in
            let rec loop () =
              let _, _ = Watch.wait_for ~xs ~timeout (Watch.any_of
                                                        (List.map (fun w -> (), w) (good_watches @ error_watches @ cancel_watches))
                                                     ) in
              let any_have_fired ws = List.fold_left (||) false (List.map (Watch.has_fired ~xs) ws) in
              (* If multiple conditions are true simultaneously then we apply the policy:
                 						   if the success condition is met then any error or cancellation is ignored
                 						   if the error condition is met then any cancellation is ignored *)
              match any_have_fired good_watches, any_have_fired error_watches, any_have_fired cancel_watches with
              | true, _, _ -> true
              | false, true, _ -> false
              | false, false, true -> Xenops_task.raise_cancelled task
              | false, false, false ->
                (* they must have fired and then fired again: retest *)
                loop () in
            loop ()
         )
    )

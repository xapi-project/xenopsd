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

(** Raised when we can't parse the output of the bootloader *)
exception Bad_sexpr of string

(** Raised when we can't parse the error from the bootloader *)
exception Bad_error of string

(** Raised when the bootloader returns an error *)
exception Error_from_bootloader of string

(** Raised when an unknown bootloader is used *)
exception Unknown_bootloader of string

val supported_bootloaders : string list
(** Bootloaders which are known to the system *)

(** Parsed representation of bootloader's stdout, as used by xend *)
type t = {kernel_path: string; initrd_path: string option; kernel_args: string}

val extract :
     Xenops_task.Xenops_task.task_handle
  -> bootloader:string
  -> disk:string
  -> ?legacy_args:string
  -> ?extra_args:string
  -> ?pv_bootloader_args:string
  -> vm:string
  -> domid:int
  -> unit
  -> t
(** Extract the default kernel from the disk *)

val delete : t -> unit
(** Delete the extracted kernel *)

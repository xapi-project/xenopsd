module Bindings = Xenctrlext_bindings.Bindings.Make(Generated)
open Bindings
open Xenctrlext_bindings.Bindings.Types
open Ctypes

module Meminfo = struct
    type t = {
      memsize: int64;
      memfree: int64
    }

    let meminfo_t = Meminfo.t

    let t =
      let read s = {
        memsize = getf s Meminfo.memsize |> Unsigned.UInt64.to_int64;
        memfree = getf s Meminfo.memfree |> Unsigned.UInt64.to_int64
      }
      in
      let write _ = failwith "UNIMPLEMENTED" in
      view ~read ~write Meminfo.t
end

module Cputopo = struct
  type t = {
    core: int;
    socket: int;
    node: int
  }
  let cputopo_t = Cputopo.t

  let t =
    let read s = {
      core = getf s Cputopo.core |> Unsigned.UInt32.to_int;
      socket = getf s Cputopo.socket |> Unsigned.UInt32.to_int;
      node = getf s Cputopo.node |> Unsigned.UInt32.to_int
    } in
    let write _ = failwith "UNIMPLEMENTED" in
    view ~read ~write Cputopo.t
end

type t = handle
let check_xc r =
  if r < 0 then failwith "TODO: xc failed"

let with_xc f =
    let h = interface_open None None Unsigned.UInt.zero in
    let r = try f h
    with e ->
      check_xc @@ interface_close h;
      raise e in
    check_xc @@ interface_close h;
    r

let numainfo xc =
  let max_nodes = allocate uint Unsigned.UInt.zero in
  check_xc @@ numainfo xc max_nodes None None;
  let nodes = Unsigned.UInt.to_int !@max_nodes in

  let meminfo = allocate_n Meminfo.t ~count:nodes in
  let distances = allocate_n Nodedist.t ~count:(nodes*nodes) in
  check_xc @@ numainfo xc max_nodes (Some (coerce (ptr Meminfo.t) (ptr Meminfo.meminfo_t) meminfo)) (Some distances);
  assert (Unsigned.UInt.to_int !@max_nodes = nodes);
  let meminfo = CArray.from_ptr meminfo nodes in
  let meminfo = Array.init nodes (fun i -> CArray.get meminfo i) in
  let distances = CArray.from_ptr distances (nodes * nodes) in
  let distances = Array.init nodes (fun i ->
    Array.init nodes (fun j -> CArray.get distances (i*nodes + j) |> Unsigned.UInt32.to_int)) in
  meminfo, distances

let cputopoinfo xc =
  let max_cpus = allocate uint Unsigned.UInt.zero in
  check_xc @@ cputopoinfo xc max_cpus None;
  let cpus = Unsigned.UInt.to_int !@max_cpus in

  let cputopo = allocate_n Cputopo.t ~count:cpus in
  check_xc @@ cputopoinfo xc max_cpus (Some (coerce (ptr Cputopo.t) (ptr Cputopo.cputopo_t) cputopo));
  assert (Unsigned.UInt.to_int !@max_cpus = cpus);
  let t = CArray.from_ptr cputopo cpus in
  Array.init cpus (CArray.get t)

let vcpu_setaffinity xc domid vcpu hard soft =
  let cpumap_of = function
    | None -> None
    | Some map ->
        let m = cpumap_alloc xc in
        List.iteri (fun i b -> if b then cpumap_setcpu i m) map;
        Some m
  in
  let flags = if hard = None then Unsigned.UInt32.zero else xen_vcpuaffinity_hard in
  let flags = if soft = None then flags else Unsigned.UInt32.logor flags xen_vcpuaffinity_soft in
  check_xc @@ vcpu_setaffinity xc (Unsigned.UInt32.of_int domid) vcpu (cpumap_of hard) (cpumap_of soft) flags

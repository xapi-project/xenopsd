(*
 * Copyright (C) 2019 Citrix Systems Inc.
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

module D = Debug.Make (struct
  let name = "topology"
end)

open D

module CPUSet = struct
  include Set.Make (struct
    type t = int

    let compare (x : int) (y : int) = compare x y
  end)

  let pp_dump = Fmt.using to_seq Fmt.(Dump.seq int)

  let to_mask t = Array.init (max_elt t) (fun i -> mem i t)

  let all n =
    n
    |> ArrayLabels.init ~f:(fun x -> x)
    |> ArrayLabels.fold_right ~f:add ~init:empty
end

module NUMAResource = struct
  type t = {affinity: CPUSet.t; memory: int64; vcpus: int}

  let v ~affinity ~memory ~vcpus =
    if Int64.compare memory 0L < 0 then
      invalid_arg
        (Printf.sprintf "NUMAResource: memory cannot be negative: %Ld" memory) ;
    if vcpus < 0 then
      invalid_arg (Printf.sprintf "vcpus cannot be negative: %d" vcpus) ;
    let n = CPUSet.cardinal affinity in
    if n < vcpus then
      invalid_arg
        (Printf.sprintf
           "Not enough CPUs in affinity set to satisfy %d vCPUs: %d" vcpus n) ;
    {affinity; memory; vcpus}

  let empty = {affinity= CPUSet.empty; memory= 0L; vcpus= 0}

  let fits ~requested ~available =
    Int64.compare requested.memory available.memory <= 0
    && CPUSet.(
         inter requested.affinity available.affinity
         |> cardinal >= requested.vcpus)

  let union a b =
    v
      ~affinity:(CPUSet.union a.affinity b.affinity)
      ~memory:(Int64.add a.memory b.memory)
      ~vcpus:(a.vcpus + b.vcpus)

  let shrink a b =
    v
      ~affinity:(CPUSet.diff a.affinity b.affinity)
      ~memory:(max 0L (Int64.sub a.memory b.memory))
      ~vcpus:(max 0 (a.vcpus - b.vcpus))

  let available t =
    Int64.compare t.memory 0L > 0
    && (not @@ CPUSet.is_empty t.affinity)
    && t.vcpus > 0

  let roundup_div64 a b = Int64.div (Int64.add a (Int64.pred b)) b

  let roundup_div a b = (a + b - 1) / b

  let min_splits_mem ~node ~vm ~max_available_mem_per_node =
    (* if this node cannot satisfy the VM's memory requirements,
     * other nodes will have to *)
    let need_from_others = max 0L (Int64.sub vm.memory node.memory) in
    (* to calculate the minimum assume all other nodes have [max_available_mem_per_node],
     * the actual number of splits might be greater than the minimum
     * *)
    let min_other_splits =
      roundup_div64 need_from_others max_available_mem_per_node
    in
    Int64.add 1L min_other_splits |> Int64.to_int

  let min_splits_cpu ~node ~vm = max 1 (roundup_div vm.vcpus node.vcpus)

  let ideal_split ~node ~vm =
    (* an ideal split is a balanced split *)
    List.fold_left max 0
      [ 1
      ; roundup_div64 vm.memory node.memory |> Int64.to_int
      ; roundup_div vm.vcpus node.vcpus ]

  let pp_dump =
    Fmt.(
      Dump.record
        [ Dump.field "affinity" (fun t -> t.affinity) CPUSet.pp_dump
        ; Dump.field "memory" (fun t -> t.memory) int64
        ; Dump.field "vcpus" (fun t -> t.vcpus) int ])
end

module NUMA = struct
  type node = Node of int

  (* no mutation is exposed in the interface,
   * therefore this is immutable *)
  type t =
    { distances: int array array
    ; cpu_to_node: node array
    ; node_cpus: CPUSet.t array
    ; all: CPUSet.t }

  let node_of_int i = Node i

  let pp_dump_distances = Fmt.(int |> Dump.array |> Dump.array)

  let v ~distances ~cpu_to_node =
    debug "Distances: %s" (Fmt.to_to_string pp_dump_distances distances) ;
    debug "CPU2Node: %s" (Fmt.to_to_string Fmt.(Dump.array int) cpu_to_node) ;
    let node_cpus = Array.map (fun _ -> CPUSet.empty) distances in
    Array.iteri
      (fun i node -> node_cpus.(node) <- CPUSet.add i node_cpus.(node))
      cpu_to_node ;
    Array.iteri
      (fun i row ->
        let d = distances.(i).(i) in
        if d <> 10 then
          invalid_arg
            (Printf.sprintf "NUMA distance from node to itself must be 10: %d"
               d) ;
        Array.iteri
          (fun _ d ->
            if d < 10 then
              invalid_arg (Printf.sprintf "NUMA distance must be >= 10: %d" d))
          row)
      distances ;
    let all = Array.fold_left CPUSet.union CPUSet.empty node_cpus in
    {distances; cpu_to_node= Array.map node_of_int cpu_to_node; node_cpus; all}

  let distance t (Node a) (Node b) = t.distances.(a).(b)

  let cpuset_of_node t (Node i) = t.node_cpus.(i)

  let node_of_cpu t i = t.cpu_to_node.(i)

  let nodes t = Array.mapi (fun i _ -> Node i) t.distances |> Array.to_list

  let all_cpus t = t.all

  let apply_mask t mask =
    let node_cpus = Array.map (CPUSet.inter mask) t.node_cpus in
    let all = CPUSet.inter t.all mask in
    {t with node_cpus; all}

  let resource t node ~memory =
    let affinity = cpuset_of_node t node in
    let vcpus = CPUSet.cardinal affinity in
    NUMAResource.v ~affinity ~memory ~vcpus

  let pp_dump_node = Fmt.(using (fun (Node x) -> x) int)

  let pp_dump =
    Fmt.(
      Dump.record
        [ Dump.field "distances"
            (fun t -> t.distances)
            (Dump.array (Dump.array int))
        ; Dump.field "cpu2node"
            (fun t -> t.cpu_to_node)
            (Dump.array pp_dump_node)
        ; Dump.field "node_cpus"
            (fun t -> t.node_cpus)
            (Dump.array CPUSet.pp_dump) ])
end

let plan host nodes ~vm =
  let host = NUMA.apply_mask host vm.NUMAResource.affinity in
  let max_numa_nodes = List.length nodes in
  let max_available_mem_per_node =
    nodes
    |> List.map (fun (_, n) -> n.NUMAResource.memory)
    |> List.fold_left max 0L
  in
  let plan_on_node (nodeidx, node) =
    let splits_mem =
      NUMAResource.min_splits_mem ~node ~vm ~max_available_mem_per_node
    in
    let splits_cpu = NUMAResource.min_splits_cpu ~node ~vm in
    let splits = min max_numa_nodes (max splits_mem splits_cpu) in
    (* an ideal split is a balanced split *)
    let ideal_splits = NUMAResource.ideal_split ~node ~vm in
    let (NUMA.Node nodei) = nodeidx in
    debug "Node %d: %d splits (%d mem; %d cpu), ideal: %d" nodei splits
      splits_mem splits_cpu ideal_splits ;
    (splits, ideal_splits, nodeidx, node)
  in
  let node_smallest_split (split1, ideal1, _, node1) (split2, ideal2, _, node2)
      =
    (* prefer smallest minimum split *)
    let r = split1 - split2 in
    if r = 0 then
      (* then pick smallest ideal split for balancing nodes *)
      let r = ideal1 - ideal2 in
      if r = 0 then
        (* then pick node with most free memory for balancing *)
        Int64.compare node2.NUMAResource.memory node1.NUMAResource.memory
      else r
    else r
  in
  let nodes =
    nodes |> List.map plan_on_node |> List.sort node_smallest_split
  in
  let ((_, _, first, _) as firstnode) = List.hd nodes in
  let node_smallest_distance (_, _, node1, _) (_, _, node2, _) =
    let d node =
      NUMA.distance host first node + NUMA.distance host node first
    in
    compare (d node1) (d node2)
  in
  let pick_node (allocated, requested) (_, _, _, candidate) =
    D.debug "requested: %s, allocated: %s"
      (Fmt.to_to_string NUMAResource.pp_dump requested)
      (Fmt.to_to_string NUMAResource.pp_dump allocated) ;
    ( NUMAResource.union allocated candidate
    , if NUMAResource.fits ~requested ~available:allocated then
        NUMAResource.empty
      else NUMAResource.shrink requested candidate )
  in
  (* we could've applied the smallest distance recursively,
   * but to avoid O(n^2) we apply it just to the first *)
  let allocated, remaining =
    nodes |> List.tl
    |> List.sort node_smallest_distance
    |> fun tl ->
    ListLabels.fold_left ~f:pick_node ~init:(NUMAResource.empty, vm)
      (firstnode :: tl)
  in
  debug "Allocated resources: %s"
    (Fmt.to_to_string NUMAResource.pp_dump allocated) ;
  debug "Requested resources: %s" (Fmt.to_to_string NUMAResource.pp_dump vm) ;
  if NUMAResource.available remaining then (
    (* There are still resources to be allocated, give up *)
    debug "Unable to allocate, remaining: %s"
      (Fmt.to_to_string NUMAResource.pp_dump remaining) ;
    None )
  else Some allocated.NUMAResource.affinity

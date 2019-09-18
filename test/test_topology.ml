open Topology

let make_numa ~numa ~sockets ~cores =
  let distances = Array.init numa (fun i ->
      Array.init numa (fun j ->
          if i = j then 10
          else 10 + 11 * abs (j - i)
  )) |> MemoryNode.Distances.of_raw in
  let cores_per_numa = cores / numa in
  let cores_per_socket = cores / sockets in
  let cpus = List.init cores (fun core ->
    let node = MemoryNode.of_raw @@ core / cores_per_numa in
    let socket = core / cores_per_socket in
    Cpu.v ~core ~socket ~node
  ) in
  Hierarchy.v cpus distances

let test_allocate ~numa ~sockets ~cores ~vm () =
  let h = make_numa ~numa ~sockets ~cores in
  prerr_endline ( Hierarchy.to_string h)


let suite = "topology test",
            ["Allocation of 1 VM on 1 node", `Quick, test_allocate ~numa:1 ~sockets:1 ~cores:1 ~vm:1
            ;"Allocation of 10 VMs on 1 node", `Quick, test_allocate ~numa:1 ~sockets:1 ~cores:8 ~vm:10
            ;"Allocation of 1 VM on 2 nodes", `Quick, test_allocate ~numa:2 ~sockets:2 ~cores:4 ~vm:1
            ;"Allocation of 10 VM on 2 nodes", `Quick, test_allocate ~numa:2 ~sockets:2 ~cores:4 ~vm:10
            ;"Allocation of 1 VM on 4 nodes", `Quick, test_allocate ~numa:4 ~sockets:2 ~cores:16 ~vm:1
            ;"Allocation of 10 VM on 4 nodes", `Quick, test_allocate ~numa:4 ~sockets:2 ~cores:16 ~vm:10
            ]

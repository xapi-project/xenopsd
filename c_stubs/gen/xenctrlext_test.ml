open Xenctrlext

let meminfo, distances = with_xc Xenctrlext.numainfo

let cputopo = with_xc Xenctrlext.cputopoinfo

let () =
  Printf.printf "Nodes: %d\n" (Array.length meminfo);
  Array.iteri (fun i mi ->
    Printf.printf "Meminfo[%d] = %Ld/%Ld\n" i
    mi.Meminfo.memfree
    mi.Meminfo.memsize) meminfo;
  Array.iteri (fun i ->
    Array.iteri (fun j e ->
      Printf.printf "%d <-> %d: %d\n" i j (Unsigned.UInt32.to_int e)
  )) distances;
  Array.iteri (fun i e ->
    Printf.printf "CPU %d -> %d, %d, %d\n"
      i e.Cputopo.core e.socket e.node
  ) cputopo

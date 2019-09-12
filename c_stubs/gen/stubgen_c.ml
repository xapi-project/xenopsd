let c_headers = "#include <xenctrl.h>"

let () =
  Format.fprintf Format.std_formatter "%s@\n" c_headers;
  Cstubs.write_c ~concurrency:Cstubs.unlocked ~prefix:"xenctrlext_stubs_" Format.std_formatter (module Xenctrlext_bindings.Bindings.Make)

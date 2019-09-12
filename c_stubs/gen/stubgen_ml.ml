let () = Cstubs.write_ml ~concurrency:Cstubs.unlocked ~prefix:"xenctrlext_stubs_" Format.std_formatter (module Xenctrlext_bindings.Bindings.Make)

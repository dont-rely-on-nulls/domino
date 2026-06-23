{
  description = "Sakura Relational Engine";
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    nix-filter.url = "github:numtide/nix-filter";
    RNT = {
      type = "github";
      owner = "mmagueta";
      repo = "RNT";
      ref = "663586b";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, nix-filter, RNT }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        legacyPackages = nixpkgs.legacyPackages.${system};
        lib = legacyPackages.lib;
        ocamlPackages = legacyPackages.ocamlPackages;
        ppx_protocol_conv =
          legacyPackages.callPackage ./deps/ppx_protocol_conv.nix {
            lib = legacyPackages.lib;
            fetchFromGitHub = legacyPackages.fetchFromGitHub;
            ocamlPackages = ocamlPackages; };
        ppx_protocol_conv_xml_light =
          legacyPackages.callPackage ./deps/ppx_protocol_conv_xml_light.nix {
            lib = legacyPackages.lib;
            ppx_protocol_conv = ppx_protocol_conv;
            fetchFromGitHub = legacyPackages.fetchFromGitHub;
            ocamlPackages = ocamlPackages; };
        rnt = RNT.packages.${system}.default;
        sources = {
          ocaml = nix-filter.lib {
            root = ./.;
            include = [
              ".ocamlformat"
              "dune-project"
              (nix-filter.lib.inDirectory "bplustree")
              (nix-filter.lib.inDirectory "bin")
              (nix-filter.lib.inDirectory "lib")
              (nix-filter.lib.inDirectory "prl_api")
              (nix-filter.lib.inDirectory "shared")
              (nix-filter.lib.inDirectory "test")
            ];
          };

          nix = nix-filter.lib {
            root = ./.;
            include = [ (nix-filter.lib.matchExt "nix") ];
          };
        };
      in {
        packages = {
          default = self.packages.${system}.relational_engine;

          relational_engine = ocamlPackages.buildDunePackage {
            pname = "sakura";
            version = "0.1.0";
            duneVersion = "3";
            src = sources.ocaml;

            buildInputs = [ ppx_protocol_conv_xml_light
                            ppx_protocol_conv
                            rnt ]
            ++ (with ocamlPackages; [
              sha
              ctypes
              ctypes-foreign
              data-encoding
              ppx_inline_test
              ppx_deriving
              ppx_sexp_conv
              lwt
              lwt-exit
              batteries
              num
            ]);

            strictDeps = true;

          };
        };
        checks = {
          relational_engine = let
            patchDuneCommand =
              let subcmds = [ "build" "test" "runtest" "install" ];
              in lib.replaceStrings
              (lib.lists.map (subcmd: "dune ${subcmd}") subcmds)
              (lib.lists.map (subcmd: "dune ${subcmd} --display=short")
                subcmds);

          in self.packages.${system}.relational_engine.overrideAttrs
          (oldAttrs: {
            name = "check-${oldAttrs.name}";
            doCheck = true;
            buildPhase = patchDuneCommand oldAttrs.buildPhase;
            checkPhase = patchDuneCommand oldAttrs.checkPhase;
            installPhase = "touch $out";
            preCheck = ''
              export CAML_LD_LIBRARY_PATH="${rnt}/lib''${CAML_LD_LIBRARY_PATH:+:$CAML_LD_LIBRARY_PATH}"
              export LD_LIBRARY_PATH="${rnt}/lib''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
              export DYLD_LIBRARY_PATH="${rnt}/lib''${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
            '';
          });

          dune-fmt = legacyPackages.runCommand "check-dune-fmt" {
            nativeBuildInputs = [
              ocamlPackages.dune_3
              ocamlPackages.ocaml
              legacyPackages.ocamlformat
            ];
          } ''
            echo "checking dune and ocaml formatting"
            dune build \
              --display=short \
              --no-print-directory \
              --root="${sources.ocaml}" \
              --build-dir="$(pwd)/_build" \
              @fmt
            touch $out
          '';

          dune-test = legacyPackages.runCommand "check-dune-test" {
            nativeBuildInputs = [
              ocamlPackages.dune_3
              ocamlPackages.ocaml
              legacyPackages.ocamlformat
              ocamlPackages.ppx_inline_test
              ocamlPackages.ppx_deriving
              ocamlPackages.ppx_sexp_conv
              ocamlPackages.lwt
              ocamlPackages.lwt-exit
              ocamlPackages.ctypes
              ocamlPackages.ctypes-foreign
              rnt
              ppx_protocol_conv
              ppx_protocol_conv_xml_light
            ];
          } ''
            echo "checking dune and ocaml formatting"
            dune build \
              --display=short \
              --no-print-directory \
              --root="${sources.ocaml}" \
              --build-dir="$(pwd)/_build" \
              @fmt
            touch $out
          '';

          # Check documentation generation
          dune-doc = legacyPackages.runCommand "check-dune-doc" {
            ODOC_WARN_ERROR = "true";
            nativeBuildInputs =
              [ ocamlPackages.dune_3 ocamlPackages.ocaml ocamlPackages.odoc ];
          } ''
            echo "checking ocaml documentation"
            dune build \
              --display=short \
              --no-print-directory \
              --root="${sources.ocaml}" \
              --build-dir="$(pwd)/_build" \
              @doc
            touch $out
          '';
        };

        devShells = {
          default = legacyPackages.mkShell {
            packages = [
              legacyPackages.nixpkgs-fmt
              legacyPackages.ocamlformat
              ocamlPackages.odoc
              ocamlPackages.ocaml-lsp
              ocamlPackages.ocamlformat-rpc-lib
              ocamlPackages.utop
              ocamlPackages.sha
              ocamlPackages.ctypes
              ocamlPackages.ctypes-foreign
              rnt
              ocamlPackages.data-encoding
              ocamlPackages.ppx_inline_test
              ocamlPackages.ppx_deriving
              ocamlPackages.ppx_sexp_conv
              ocamlPackages.lwt
              ocamlPackages.lwt-exit
              ocamlPackages.batteries
              ocamlPackages.num
              ppx_protocol_conv
              ppx_protocol_conv_xml_light
              ocamlPackages.earlybird
              legacyPackages.coq
              legacyPackages.coqPackages.stdlib
              legacyPackages.z3
            ];

            shellHook = ''
              export CAML_LD_LIBRARY_PATH="''${CAML_LD_LIBRARY_PATH:+$CAML_LD_LIBRARY_PATH:}$(ocamlfind query num)"
              export RNT_ROOT="${rnt}"
              export CPATH="${rnt}/include''${CPATH:+:$CPATH}"
              export LIBRARY_PATH="${rnt}/lib''${LIBRARY_PATH:+:$LIBRARY_PATH}"
              export DYLD_LIBRARY_PATH="${rnt}/lib''${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
              export LD_LIBRARY_PATH="${rnt}/lib''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
            '';

            inputsFrom = [ self.packages.${system}.relational_engine ];
          };
        };
      });
}

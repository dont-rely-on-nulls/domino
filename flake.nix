{
  description = "Domino's flakes";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";

    flake-parts = {
      url = "github:hercules-ci/flake-parts";
    };

    treefmt-nix.url = "github:numtide/treefmt-nix";
  };

  outputs = { self, nixpkgs, flake-parts, treefmt-nix, ... }@inputs:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];

      perSystem = { pkgs, system, ... }: 
        let
          erlangLatest = pkgs.erlang_28; 
          
          rebar_config = builtins.readFile ./rebar.config;
          match = builtins.match ".*release,.+domino, \"([0-9.]+)\".*" rebar_config;
          pversion = builtins.head match;
        in {
          # This sets `pkgs` to a nixpkgs with allowUnfree option set.
          _module.args.pkgs = import nixpkgs {
            inherit system;
            config.allowUnfree = true;
          };

          # nix build
          packages = {
            default =
              let
                deps = import ./rebar-deps.nix {
                  inherit (pkgs) fetchHex fetchFromGitHub fetchgit;
                  builder = pkgs.beamPackages.buildRebar3;
                };
              in
              pkgs.beamPackages.rebar3Relx {
                pname = "domino";
                version = pversion;
                src = pkgs.lib.cleanSource ./.;
                releaseType = "release";
                profile = "prod";
                buildInputs = (
                  with pkgs;
                  [
                    coreutils
                    gawk
                    gnugrep
                    openssl
                  ]
                  ++ lib.optional stdenv.isLinux [
                    liburing
                  ]
                );
                beamDeps = builtins.attrValues deps;
              };
          };

          # Define devShell for Erlang + Rebar3
          devShells = {
            default = pkgs.mkShell {
              packages = with pkgs; [
                erlang-language-platform
                rebar3
              ] ++ [erlangLatest];
            };
          };

          # Formatter setup for Nix files
          formatter = treefmt-nix.lib.evalModule pkgs ./treefmt.nix;

          # nix flake check --all-systems
          checks = {
            formatting = treefmt-nix.lib.evalModule pkgs ./treefmt.nix;
          };
        };
    };
}

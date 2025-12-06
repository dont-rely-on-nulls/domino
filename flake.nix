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
          erlang = pkgs.erlang_28; 

          buildRebar3 = pkgs.beamPackages.buildRebar3 {
            pname = "domino";     
            version = "0.1.0";
            src = ./.;           
            beam = erlang;  
            generate-lock = true; 
            name = "domino";     
            buildInputs = [ pkgs.rebar3 ];  

            meta = with pkgs.lib; {
              description = "Domino Erlang Server";
              license = pkgs.lib.licenses.mit;
              platforms = pkgs.lib.platforms.all;
            };
          };

        in {
          # This sets `pkgs` to a nixpkgs with allowUnfree option set.
          _module.args.pkgs = import nixpkgs {
            inherit system;
            config.allowUnfree = true;
          };

          # nix build
          packages = {
            domino = buildRebar3;  
            default = buildRebar3; 
          };

          # Define devShell for Erlang + Rebar3
          devShells = {
            default = pkgs.mkShell {
              packages = with pkgs; [
                erlang_28
                erlang-language-platform
                rebar3
              ];
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

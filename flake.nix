{
  description = "Domino's flakes";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";

    flake-parts = {
      url = "github:hercules-ci/flake-parts";
    };

    treefmt-nix.url = "github:numtide/treefmt-nix";
  };

  outputs = 
    { self, 
      nixpkgs,
      flake-parts,
      treefmt-nix,
      ... 
    }@inputs:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];

      perSystem =
        { pkgs, system, ... }:
          let
            treefmtEval = treefmt-nix.lib.evalModule pkgs ./treefmt.nix;
          in
          {
            # This sets `pkgs` to a nixpkgs with allowUnfree option set.
            _module.args.pkgs = import nixpkgs {
              inherit system;
              config.allowUnfree = true;
            };

            # nix build
            packages = {
              hello = pkgs.hello;
              default = pkgs.hello;
            };

            devShells = 
              {
                default = pkgs.mkShell {
                  packages = with pkgs; [
                    erlang_28
                    erlang-language-platform
                    rebar3
                  ];
                };
              };

            # nix fmt
            formatter = treefmtEval.config.build.wrapper;

            # nix flake check --all-systems
            checks = {
              formatting = treefmtEval.config.build.check self;
            };
          };
    };
}

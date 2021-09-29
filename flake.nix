{

  inputs = {
    nixpkgs = {
      type = "github";
      owner = "NixOS";
      repo = "nixpkgs";
      rev = "332ad080eac7fdd90ee653c26b5f9d3a7cb97a1b";
    };
  };

  outputs = { self, nixpkgs }: {

    defaultPackage.x86_64-linux = nixpkgs.legacyPackages.x86_64-linux.stdenv.mkDerivation {
        name = "sqlite3-example";
        src = ./.;
        buildInputs = [ 
            nixpkgs.legacyPackages.x86_64-linux.sqlite
          ];
        installFlags = [ "DESTDIR=$(out)" "PREFIX=/" ];
    };

  };
}

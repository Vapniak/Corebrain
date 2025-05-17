import os
import subprocess
import sys
from pathlib import Path

SWIG_TARGETS = {
    "csharp": {
        "flags": "-csharp -dllimport corebrain",
        "outfile": "csharp_wrap.cxx",
        "required_files": ["corebrain.cs", "corebrainPINVOKE.cs"]
    }
}

def generate_bindings():
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.parent.parent
    
    # 1. Generate C header from Cython
    try:
        subprocess.run([
            "cython",
            "--cplus",
            "-3",
            "-o", str(project_root/"corebrain"/"corebrain.cpp"),
            str(project_root/"corebrain"/"corebrain.pyx"),
            "--embed=hints"
        ], check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Cython failed: {e}")
        sys.exit(1)

    # 2. Generate language bindings
    for lang, config in SWIG_TARGETS.items():
        out_dir = project_root/"corebrain"/"bindings"/lang
        out_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            subprocess.run([
                "swig",
                *config["flags"].split(),
                "-outdir", str(out_dir),
                "-o", str(script_dir/config["outfile"]),
                str(script_dir/"corebrain.i")
            ], check=True)
            
            # Verify output files
            for f in config["required_files"]:
                if not (out_dir/f).exists():
                    raise FileNotFoundError(f"SWIG failed to generate {f}")
            
            print(f"✅ {lang.upper()} bindings generated in {out_dir}")
            
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            print(f"❌ SWIG failed for {lang}: {e}")
            sys.exit(1)

if __name__ == "__main__":
    generate_bindings()
# elzorro
Mask generation for large galaxy surveys

## Prototype implementation

### Import pipeline

You can run the import pipleine with `import_pipeline.ipynb` Jupyter notebook.
The pre-requisites are:

1. Have the python environment with Jupyter/ipykernel activated, e.g. with conda on PSC:
```bash
conda create -p./cenv python=3.11
conda activate ./cenv
pip install ipykernel
python -m ipykernel install --user --name=elzorro
# Reload Jupyter tab in your browser
```
2. Setup Rust toolchain for `mom_builder` package build (this installs the toolchain for your user, but conda installation should also work):
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# Follow the instructions, e.g. add `source $HOME/.cargo/env` to your shell profile
```
3. Install the required packages:
```bash
python -m pip install -r requirements.txt
```

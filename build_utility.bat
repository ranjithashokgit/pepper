cd .
python setup.py sdist bdist_wheel
pip uninstall pepper_fusion -y
cd dist
REM Loop through and install the pepper_fusion wheel files
for %%f in (pepper_fusion*.whl) do pip install %%f

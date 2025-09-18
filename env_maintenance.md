# Environment Maintenance Tips

If you add, remove, or update packages in the environment, please **regenerate and commit the lock file** so others can reproduce your setup exactly.

Make sure to run the below commands **from inside the activated environment**:
```bash
conda activate cymphony4cs
```

## Add or remove packages:

```bash
# Add a new conda package
conda install <package-name>

# OR: Remove a package
conda remove <package-name>

# Add a pip package (inside the conda environment)
pip install <pip-package>
```

## Regenerate and commit the environment.lock.yml file:
```bash
# Export exact versions of all packages in the current environment
conda env export --no-builds -n cymphony4cs > environment.lock.yml

# Commit the changes
git add environment.lock.yml
git commit -m "Update lock file after adding/removing packages"
git push
```

> `environment.lock.yml` pins exact versions of all packages and pip dependencies.  
> Committing this file ensures all collaborators and deployments use the same environment.




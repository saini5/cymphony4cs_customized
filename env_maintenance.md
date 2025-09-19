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
1. Export exact versions of all packages in the current environment:  
    ```bash
    conda env export --no-builds -n cymphony4cs > environment.lock.yml
    ```
2. **Remove the prefix: line at the end of the environment.lock.yml**, as it contains a machine-specific path and will break portability:
    ```bash
    prefix: C:\Users\...
    ```
    - Open environment.lock.yml in any editor
    - Delete the line that starts with prefix:
3. Commit the changes:  
    ```bash
    git add environment.lock.yml
    git commit -m "Update lock file after adding/removing packages"
    git push
    ```

> `environment.lock.yml` pins exact versions of all packages and pip dependencies.  
> Committing this file ensures all collaborators and deployments use the same environment.




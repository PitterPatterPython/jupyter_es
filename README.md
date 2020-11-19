# jupyter_es
A module to help interaction with Jupyter Notebooks and Elastic Search Clusters

------
This is a python module that helps to connect Jupyter Notebooks to various datasets. 
It's based on (and requires) https://github.com/JohnOmernik/jupyter_integration_base 



## Initialization 
----

### Example Inits

#### Embedded mode using qgrid
```
from es_core import Es
ipy = get_ipython()
Es = Es(ipy, debug=False, pd_display_grid="qgrid")
ipy.register_magics(Es)
```


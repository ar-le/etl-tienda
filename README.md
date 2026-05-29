<h3>Cómo levantar el proyecto</h3>
<ol>
  <li>En un virtual environment de python, instalar requirements.txt</li>
  <li>En la terminal ejecutar 
    <pre>export DAGSTER_HOME=$(pwd)/.dagster_home</pre>
    (Opcional, si no se hace se perderán los datos de las runs al volver a levantar)
  <li>Desde el venv, ejecutar dagster dev</li>
  <li>En definitions.py, modificar las constantes con la información de Hive (IP, usuario, base de datos), y las rutas a los csvs</li>
  <li>Acceder a localhost:3000</li>
  <li>Entrar en Jobs > full_etl > Materialize all  para lanzar una run</li>
</ol>

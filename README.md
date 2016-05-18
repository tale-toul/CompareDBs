# SCRIPT DE COMPARACION DE LAS BASES DE DATOS DE "MASTER" Y ESCLAVO

El script necesita conectarse a las dos bases de datos usando un usuario, 
puesto que este usuario solo tiene que leer datos y no escribir nada, creamos 
un nuevo usuario de sololectura, con la orden SQL:
```sql
MariaDB> grant select,show databases on *.* to 'mogutu'@'%' identified by 'XXXXX';
```
Este usuario puede conectarse desde culaquier IP, no está limitado a un origen 
concreto, más adelante puede que cambiemos esto.

La lógica general del script en una base de datos es:

* Conectar al servidor de base de datos.
* Obtener la lista de bases de datos.  
    `show databases;`

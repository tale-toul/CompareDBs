## SCRIPT DE COMPARACION DE BASES DE DATOS REPLICADAS ENTRE "MASTER" Y ESCLAVO

### Lógica general del programa

El script necesita conectarse a las dos bases de datos usando un usuario, puesto que este
usuario solo tiene que leer datos y no escribir nada, creamos un nuevo usuario de
sololectura, con la orden SQL:

```sql
MariaDB> grant select,show databases on *.* to 'mogutu'@'%' identified by 'XXXXX';
```

Este usuario puede conectarse desde culaquier IP, no está limitado a un origen concreto,
más adelante puede que cambiemos esto.

La lógica general del script en una base de datos es:

* Conectar al servidor de base de datos.
* Obtener la lista de bases de datos.  
    `>show databases;`
* Para cada base de datos 
    * Obtener la lista de tablas  
        `>show tables;`
    * Contar las filas de la tabla  
        `>SELECT COUNT(*) FROM <TABLE>;`

La recopilación de la información se repite para cada base de datos.
Toda la información obtenida de la base de datos se almacena en un diccionario.  

Cuando tenemos la información de las dos bases de datos se compara y muestra en pantalla.
Si se detecta alguna diferencia se muestra un mensaje de error y se detiene la
comparación en ese punto.

### Diferencias de la información durante la ejecución

El script usa un usuario de solo lectura para conectarse a las bases de datos, y no
bloquea las tablas mientras obtiene los datos, esto quiere decir que es posible que
mientras que se extrae la información se produzca algún cambio que genere una alarma
indicando que los datos no coinciden entre _master_ y esclavo.

### Velocidad de ejecución

En la primera versión (**v0.1**) del script la recopilación de la información se hace de
forma secuencial, primero del _master_ y después del esclavo.  Para una cantidad de
datos pequeña, como tenemos actualmente (menos de **200MB**) el escript se ejecuta en
menos de 2 segundos, pero a medida que el volumen de información crezca, también lo
hará el tiempo de ejecución, y como se ha visto en el [apartado anterior](###
Velocidad de ejecución) aumenta la probabilidad de que se produzcan diferencias entre
las dos bases de datos.

Una posible mejora para acelerar la ejecución es consultar los datos de los dos
servidores en paralelo, usando _multiprocessing_

### Recopilación de datos desde una función

**version 1.0**

Puesto que la recopilación de datos de los dos servidores de bases de datos es identica,
se convierte el código de obtención de datos en una función `collect_data_from_base` a la
que se le pasan como parametros: el usuario, la contraseña y la máquina donde están las
bases de datos.

### Ejecución en paralelo

**versión 1.1**

En esta versión se modifica la ejecución del programa para que las consultas a las bases
de datos se hagan en paralelo.  En lugar de esperar a que termine una consulta y después
lanzar la siguiente, ambas se lanzan a la misma vez.

La ejecución en paralelo se hace usando el modulo de python _multiprocessing_ y el
proceso que se lanza en paralelo es la función que se definió en la versión [1.0](###
Recopilación de datos desde una función)

Los resultados devueltos por cada proceso paralelo se guardan en una cola, de la que son
recogidos y comprobadas.  

### Comprobación y mostrado de resultados

La comprobación de igualdad es muy simple en python, si r1 y 42 son los diccionarios
obtenidos, es suficiente con:

`if r1 == r2:` 

Si el resultado es **False** la busqueda de las diferencias es más complicada y se hace
desde la función `show_diffs`.  Esta función compara y muestra los nombres de los
servidores, bases de datos, tablas y cuenta de filas.  Si encuentra alguna diferencia se
detiene a la espera de que se pulse una tecla.

Queda pendiente modificar este script para que se pueda ejecutar como chequeo de nagios.






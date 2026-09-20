Redactar un breve informe en el archivo `INFORME.md` explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes, grándes volúmens de datos y la cantidad de controles.

## Middleware (implementado)

Se conserva la interfaz provista y se agregan dos métodos a las implementaciones
RabbitMQ:

- `send_to_queue(queue_name, message)` declara una cola durable antes de la
  primera publicación a ese destino y reutiliza la conexión existente. Espera
  confirmación del broker y detecta publicaciones sin destino mediante
  `mandatory=True`. El método `send(message)` de la implementación de colas usa
  este mismo mecanismo.
- `register_consumer(queue_name, callback)` declara una cola adicional y guarda
  su callback. Se invoca antes de `start_consuming(callback_principal)`, que
  atiende todos los consumidores secuencialmente. `stop_consuming()` los detiene
  a todos; las registraciones se conservan para un nuevo inicio.

Cada consumidor tiene prefetch de un mensaje y acknowledgements manuales. La
conexión pertenece al proceso que crea el middleware; no se comparte entre
procesos ni requiere threads para atender varias colas. Los callbacks deben
evitar trabajos prolongados que impidan atender los eventos de la conexión.

Las publicaciones usan publisher confirms. Una confirmación significa que el
broker aceptó el mensaje, no que el consumidor terminó de procesarlo. La clase
de exchanges conserva sus suscripciones temporales y no exige destinatarios
para su método `send`.

Ante errores de comunicación, el middleware intenta cerrar su conexión y
propaga el error. No se implementan reconexiones ni reintentos automáticos.

## Aislamiento de consultas y flujo con una réplica

Cada `MessageHandler` crea un identificador de consulta antes de que el Gateway
lo copie a sus procesos. Los mensajes internos incluyen ese identificador y un
tipo explícito: datos, fin de registros, top parcial o resultado final. El EOF
del Gateway lleva la cantidad de registros enviados. El handler de respuesta
solo devuelve los resultados de su propia consulta; devuelve `None` para las
ajenas. Para un top vacío coincidente devuelve una lista con valor booleano
verdadero, porque el Gateway fijo usa esa condición para decidir si encontró al
destinatario, mientras que el protocolo externo usa la longitud de la lista.

```mermaid
sequenceDiagram
    participant G as Gateway / MessageHandler
    participant S as Sum
    participant A as Aggregation
    participant J as Join
    G->>S: DATA(consulta, fruta, cantidad)
    G->>S: EOF(consulta, cantidad de registros)
    S->>A: DATA(consulta, fruta, acumulado)
    S->>A: EOF(consulta, cantidad de parciales)
    A->>J: PARTIAL_TOP(consulta, réplica, pares)
    J->>G: RESULT(consulta, pares)
```

El diagrama resume una consulta; mensajes de consultas distintas pueden
intercalarse. Sum y Aggregation mantienen acumulados y contadores separados por
consulta. Con una réplica por etapa, cada EOF se procesa después de los datos de
su consulta y se verifica su contador antes de publicar la salida. Los parciales
y su marcador viajan por la misma cola de Aggregation, declarada antes de enviar.
Cada proceso reutiliza su conexión para consumir y publicar.

La acumulación utiliza `FruitItem.__add__`. Aggregation selecciona los mayores
elementos recién después de consolidar las cantidades, usando las comparaciones
de `FruitItem`. Join identifica los emisores de cada top y conserva como máximo
`TOP_SIZE` candidatos entre recepciones. El estado de una consulta se elimina
después de confirmar la publicación de su salida.

La lógica común de acumulación y selección está en `common/processing.py`; el
consumo, cierre y manejo de SIGTERM/SIGINT están en `common/control.py`. Los
callbacks se ejecutan secuencialmente en el proceso propietario del estado, por
lo que no hay actualizaciones concurrentes que requieran un mutex local. Ante
errores se registra la causa, se cierran los recursos y el control termina con
código distinto de cero.

## Coordinación de las réplicas de Sum

Las réplicas consumen datos de la misma cola de entrada. Cada una mantiene sus
acumulados por consulta y atiende además una cola de control cuyo nombre se
deriva de `SUM_PREFIX` e `ID`. No se crean colas por cliente. La réplica que
recibe el EOF del Gateway coordina exclusivamente el cierre de esa consulta;
también sigue participando como trabajadora.

El cierre tiene dos barreras:

1. **Procesamiento:** el coordinador envía `PREPARE` a todas las Sum. Cada una
   responde con `PROGRESS`, incluyendo su contador acumulado de registros
   procesados, incluso si es cero. Si termina un registro después de responder,
   envía el contador actualizado. El coordinador espera informes de todas las
   réplicas y una suma exactamente igual al total anunciado por el Gateway.
2. **Publicación:** el coordinador envía `FLUSH`. Cada réplica publica sus
   parciales hacia Aggregation y, una vez confirmados por RabbitMQ, responde
   `FLUSHED` con la cantidad de parciales. Solo cuando respondieron todas se
   publica un único EOF hacia Aggregation con la cantidad total de parciales.

Se trata de un protocolo de finalización con dos barreras. Si una réplica falla después de publicar algunos parciales, no se revierten esas publicaciones ni se garantiza completar la consulta.

Las barreras coordinan procesos independientes mediante RabbitMQ.
`CompletionBarrier` es el estado local que el coordinador utiliza para registrar
los informes y comprobar las condiciones de avance. El protocolo actual permite
seguir atendiendo mensajes mientras se esperan respuestas de otras réplicas.

```mermaid
sequenceDiagram
    participant G as Gateway
    participant C as Sum que recibe EOF
    participant S as Cada réplica Sum (incluye C)
    participant A as Aggregation
    G->>C: EOF(consulta, N)
    C->>S: PREPARE(consulta, coordinador)
    S->>C: PROGRESS(consulta, procesados)
    opt Finaliza un registro pendiente después del informe
        S->>C: PROGRESS(consulta, contador actualizado)
    end
    Note over C: Respondieron todas y suma de contadores = N
    C->>S: FLUSH(consulta)
    S->>A: Parciales confirmados por el broker
    S->>C: FLUSHED(consulta, cantidad de parciales)
    Note over C: Todas terminaron de publicar
    C->>A: EOF(consulta, total de parciales)
```

La primera barrera no supone orden entre la cola de control y la de datos. La
prueba de finalización se basa en los contadores. Para acotar el tráfico se usa
que cada Gateway publica sus registros y EOF por el mismo canal y cola, sin
prioridades, y cada consumidor de datos tiene prefetch 1 y confirma después de
procesar. Al extraerse el EOF, los registros anteriores ya fueron entregados;
queda a lo sumo uno pendiente por réplica. Por eso, después de `PREPARE`, cada
réplica necesita un informe inicial y como máximo una actualización. Este
argumento corresponde a ejecución sin fallas ni reentregas: no se implementa
recuperación después de una desconexión.

En la segunda barrera, las confirmaciones del broker establecen que todos los
parciales llegaron a su cola antes de publicar el EOF. Aggregation tiene un
único consumidor secuencial y recibe el marcador por esa misma cola, por lo que
no calcula el top mientras queden parciales anteriores por procesar.

Con S réplicas, el cierre requiere a lo sumo `5*S + 1` publicaciones de control
internas por consulta: preparación, hasta dos informes de progreso, orden de
publicación, confirmación de publicación y un EOF hacia Aggregation. Se
excluyen el EOF original y los acknowledgements del transporte. Los mensajes
llevan identificadores y contadores, no listas de registros ni de réplicas. El
coordinador mantiene O(S) estado y actualiza los totales incrementalmente, sin
recorrer todos los contadores en cada informe. Los contadores requieren una
cantidad de dígitos proporcional al logaritmo del volumen representado.

No se espera bloqueando dentro de un callback a que otras réplicas respondan.
Cada mensaje actualiza el estado y retorna al consumo. Los datos de consultas
distintas pueden seguir intercalándose. El estado local se elimina después de
publicar `FLUSHED`, y el de coordinación después de publicar el EOF final.

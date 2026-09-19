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

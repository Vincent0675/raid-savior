from collections import deque
from typing import Any
import threading

class SSEBus:
    """
    BUS para broadcasting de eventos a múltiples clientes SSE.
    Cada cliente tiene su propia cola (deque) de eventos.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._subscribers: list[deque[dict[str, Any]]] = []

    def subscribe(self) -> deque[dict[str, Any]]:
        """
        Crea una cola para un nuevo cliente y la regista.
        Devuelve la deque para que el endpoint SSE la use.
        """
        q: deque[dict[str, Any]] = deque()
        with self._lock:
            self._subscribers.append(q)
        return q

    def unsubscribe(self, q: deque[dict[str, Any]]) -> None:
        """
        Elimina la cola del cliente cuando este se desconecta.
        """
        with self._lock:
            if q in self._subscribers:
                self._subscribers.remove(q)
    
    def publish(self, event_dict: dict[str, Any]) -> None:
        """
        Envía un evento (dict JSON-serializable) a todas las colas activas.
        """
        with self._lock:
            for q in self._subscribers:
                q.append(event_dict)

# Instancia global para usar desde receiver.py / app.py
sse_bus = SSEBus()
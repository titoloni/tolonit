from typing import List, Optional
import logging

# Configurando o nível de log e o formato da mensagem
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# =============================================================================#
def get_partition_date(
    param_date: Optional[int], partitions_dates: List[int], date_type: str
) -> int:
    """
    Retorna a data da partição com base nos parâmetros fornecidos.

    Parâmetros:
    param_date (Optional[int]): Data de partição fornecida pelo usuário.
    partitions_dates (List[int]): Lista de datas de partição disponíveis.
    date_type (str): Tipo de data a ser retornada ('last_date' ou outra).

    Retorna:
    int: A data da partição apropriada.

    Levanta:
    ValueError: Se a última ou a data anterior não puderem ser encontradas.
    """
    if param_date is not None:
        return param_date

    last_date = max(partitions_dates, default=0)
    if last_date == 0:
        _log_and_raise("Não foi possível recuperar a data da última partição de cnpj9.")

    if date_type == "last_date":
        return last_date

    prev_date = max((date for date in partitions_dates if date < last_date), default=0)
    if prev_date == 0:
        _log_and_raise(
            "Não foi possível recuperar a data anterior da partição de cnpj9."
        )

    return prev_date


def _log_and_raise(message: str):
    """Registra um erro e levanta uma exceção ValueError."""
    logging.error(message)
    raise ValueError(message)
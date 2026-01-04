"""
Funções auxiliares para gerenciamento de datas nos DAGs
"""
from datetime import datetime
from typing import Tuple, Optional
import logging

logger = logging.getLogger(__name__)


def get_latest_epa_year_dynamic() -> int:
    """
    Consulta dinamicamente o último ano disponível na EPA.
    
    Returns:
        Último ano com dados EPA disponíveis
    """
    try:
        from hooks.epa_hook import EPAHook
        
        hook = EPAHook()
        latest_year = hook.get_latest_year()
        
        logger.info(f"✅ Dynamically discovered EPA year: {latest_year}")
        return latest_year
        
    except Exception as e:
        logger.error(f"❌ Failed to get EPA year dynamically: {e}")
        # Fallback para estimativa
        return get_latest_epa_year_estimated()


def get_latest_epa_year_estimated() -> int:
    """
    Estima o último ano EPA baseado em regras (fallback).
    
    Regra: Ano atual - 2 anos (É o padrão conhecido)
    """
    current_year = datetime.now().year
    estimated = current_year - 2
    #estimated = 2023 -> para testes manuais
    logger.warning(f"⚠️ Using estimated EPA year: {estimated}")
    return estimated


def get_safe_data_period(use_dynamic: bool = True) -> Tuple[str, str]:
    """
    Retorna período seguro onde TODAS as APIs têm dados disponíveis.
    
    Args:
        use_dynamic: Se True, consulta EPA dinamicamente; 
                     Se False, usa ano fixo/estimado
    
    Returns:
        Tuple[str, str]: (start_period, end_period) no formato "YYYY-MM"
    
    Examples:
        >>> get_safe_data_period(use_dynamic=True)
        ('2023-01', '2023-12')  # Baseado em consulta real à EPA
        
        >>> get_safe_data_period(use_dynamic=False)
        ('2024-01', '2024-12')  # Baseado em estimativa
    """
    if use_dynamic:
        try:
            year = get_latest_epa_year_dynamic()
        except Exception as e:
            logger.error(f"Dynamic lookup failed, using estimated: {e}")
            year = get_latest_epa_year_estimated()
    else:
        year = get_latest_epa_year_estimated()
    
    start_period = f"{year}-01"
    end_period = f"{year}-12"
    
    logger.info(f"📅 Safe data period: {start_period} to {end_period}")
    
    return start_period, end_period


def format_period_range(start_period: str, end_period: str) -> dict:
    """
    Converte período YYYY-MM para diferentes formatos usados nas APIs.
    """
    import calendar
    from datetime import datetime
    
    start_dt = datetime.strptime(start_period, "%Y-%m")
    end_dt = datetime.strptime(end_period, "%Y-%m")
    
    start_date = f"{start_dt:%Y-%m}-01"
    last_day = calendar.monthrange(end_dt.year, end_dt.month)[1]
    end_date = f"{end_dt.year:04d}-{end_dt.month:02d}-{last_day:02d}"
    
    return {
        "start_period": start_period,
        "end_period": end_period,
        "start_date": start_date,
        "end_date": end_date,
        "start_ts": f"{start_date}T00",
        "end_ts": f"{end_date}T23",
        "year": start_dt.year
    }

from __future__ import annotations

from datetime import datetime
import pytest


def calculate_duration_minutes(pickup: datetime | None, dropoff: datetime | None) -> float | None:
    if not pickup or not dropoff:
        return None
    return (dropoff - pickup).total_seconds() / 60.0


def evaluate_quality_rules(
    pickup: datetime | None,
    dropoff: datetime | None,
    distance: float | None,
    total_amount: float | None,
) -> tuple[bool, str | None]:
    reasons = []
    if pickup is None:
        reasons.append("pickup_datetime_nula")
    if dropoff is None:
        reasons.append("dropoff_datetime_nula")
    if pickup and dropoff and dropoff <= pickup:
        reasons.append("desembarque_antes_ou_igual_embarque")
    if pickup and dropoff and (dropoff - pickup).total_seconds() <= 0:
        reasons.append("duracao_nao_positiva")
    if distance is None or distance < 0:
        reasons.append("distancia_nula_ou_negativa")
    if total_amount is None or total_amount < 0:
        reasons.append("valor_total_nulo_ou_negativo")

    is_valid = len(reasons) == 0
    reason_str = ";".join(reasons) if reasons else None
    return is_valid, reason_str


def evaluate_anomalies(duration_minutes: float | None, distance: float | None) -> tuple[bool, str | None]:
    reasons = []
    if duration_minutes is not None and duration_minutes > 360.0:
        reasons.append("duracao_acima_6_horas")
    if distance is not None and distance > 100.0:
        reasons.append("distancia_acima_100_milhas")

    is_anomaly = len(reasons) > 0
    reason_str = ";".join(reasons) if reasons else None
    return is_anomaly, reason_str


def evaluate_valid_revenue(is_valid_trip: bool, is_valid_payment: bool, total_amount: float) -> float:
    if is_valid_trip and is_valid_payment:
        return max(0.0, total_amount)
    return 0.0


def test_valid_trip_evaluation():
    p = datetime(2025, 1, 15, 10, 0, 0)
    d = datetime(2025, 1, 15, 10, 20, 0)
    is_valid, reason = evaluate_quality_rules(p, d, distance=3.5, total_amount=18.5)

    assert is_valid is True
    assert reason is None


def test_invalid_trip_negative_distance_and_reverse_timestamps():
    p = datetime(2025, 1, 15, 10, 30, 0)
    d = datetime(2025, 1, 15, 10, 10, 0)  # desembarque antes de embarque
    is_valid, reason = evaluate_quality_rules(p, d, distance=-1.0, total_amount=20.0)

    assert is_valid is False
    assert "desembarque_antes_ou_igual_embarque" in reason
    assert "distancia_nula_ou_negativa" in reason


def test_invalid_trip_null_timestamps():
    is_valid, reason = evaluate_quality_rules(None, None, distance=2.0, total_amount=15.0)
    assert is_valid is False
    assert "pickup_datetime_nula" in reason
    assert "dropoff_datetime_nula" in reason


def test_anomaly_duration_above_6_hours():
    duration = 400.0  # > 360 minutos (6 horas)
    distance = 15.0
    is_anomaly, reason = evaluate_anomalies(duration, distance)

    assert is_anomaly is True
    assert reason == "duracao_acima_6_horas"


def test_anomaly_distance_above_100_miles():
    duration = 45.0
    distance = 150.0  # > 100 milhas
    is_anomaly, reason = evaluate_anomalies(duration, distance)

    assert is_anomaly is True
    assert reason == "distancia_acima_100_milhas"


def test_anomaly_both_duration_and_distance():
    duration = 500.0
    distance = 120.0
    is_anomaly, reason = evaluate_anomalies(duration, distance)

    assert is_anomaly is True
    assert "duracao_acima_6_horas" in reason
    assert "distancia_acima_100_milhas" in reason


def test_valid_revenue_amount_logic():
    # Caso 1: Viagem válida com cartão de crédito (válido)
    assert evaluate_valid_revenue(True, True, 35.50) == 35.50

    # Caso 2: Viagem válida com pagamento disputa (inválido)
    assert evaluate_valid_revenue(True, False, 35.50) == 0.0

    # Caso 3: Viagem inválida mesmo com cartão de crédito
    assert evaluate_valid_revenue(False, True, 35.50) == 0.0

    # Caso 4: Viagem inválida e pagamento inválido
    assert evaluate_valid_revenue(False, False, 35.50) == 0.0

#!/usr/bin/env python3
"""
Análise manual dos cálculos de outliers para entender por que valores altos
como 1251.9h não são considerados outliers pelo método IQR.
"""

import json


def analyze_outliers():
    # Carregar dados do JSON
    with open("output/cycle-time/cycle_time_CWS_20250922_162029.json", "r") as f:
        data = json.load(f)

    # Extrair cycle times
    cycle_times = []
    for issue in data["issues"]:
        cycle_times.append(issue["cycle_time_hours"])

    # Ordenar os valores
    cycle_times_sorted = sorted(cycle_times)
    print("Valores de Cycle Time (em horas):")
    for i, ct in enumerate(cycle_times_sorted, 1):
        print(f"{i}: {ct:.2f}h ({ct / 24:.1f} dias)")

    print("\n=== CÁLCULO MANUAL DO IQR ===")
    n = len(cycle_times_sorted)
    print(f"Total de valores: {n}")

    # Calcular quartis usando o método padrão Python (similar ao statistics.quantiles)
    # Para n=8, as posições são:
    # Q1: posição 2.25 (entre 2º e 3º valores)
    # Q3: posição 6.75 (entre 6º e 7º valores)

    q1_pos = (n + 1) * 0.25  # 2.25
    q3_pos = (n + 1) * 0.75  # 6.75

    # Interpolação linear
    q1_idx_low = int(q1_pos) - 1  # índice 1 (2º valor)
    q1_idx_high = q1_idx_low + 1  # índice 2 (3º valor)
    q1_fraction = q1_pos - int(q1_pos)  # 0.25

    q3_idx_low = int(q3_pos) - 1  # índice 5 (6º valor)
    q3_idx_high = q3_idx_low + 1  # índice 6 (7º valor)
    q3_fraction = q3_pos - int(q3_pos)  # 0.75

    q1 = cycle_times_sorted[q1_idx_low] + q1_fraction * (
        cycle_times_sorted[q1_idx_high] - cycle_times_sorted[q1_idx_low]
    )
    q3 = cycle_times_sorted[q3_idx_low] + q3_fraction * (
        cycle_times_sorted[q3_idx_high] - cycle_times_sorted[q3_idx_low]
    )

    print(f"Q1 (25º percentil): {q1:.2f}h")
    print(f"Q3 (75º percentil): {q3:.2f}h")

    iqr = q3 - q1
    print(f"IQR: {iqr:.2f}h")

    # Calcular limites de outliers (k=1.5 padrão)
    k = 1.5
    lower_bound = q1 - k * iqr
    upper_bound = q3 + k * iqr

    print(f"Limite inferior: {lower_bound:.2f}h")
    print(f"Limite superior: {upper_bound:.2f}h")

    print("\n=== ANÁLISE DE OUTLIERS ===")
    outliers = []
    for i, ct in enumerate(cycle_times_sorted):
        if ct < lower_bound or ct > upper_bound:
            outliers.append((i + 1, ct))
            print(f"OUTLIER: Posição {i + 1}, Valor: {ct:.2f}h ({ct / 24:.1f} dias)")

    if not outliers:
        print("❌ NENHUM OUTLIER DETECTADO")
        print("Todos os valores estão dentro dos limites:")
        for i, ct in enumerate(cycle_times_sorted):
            status = "✅" if lower_bound <= ct <= upper_bound else "❌"
            print(f"  {status} Valor {i + 1}: {ct:.2f}h")

    print(f"\n=== EXPLICAÇÃO ===")
    print(f"O método IQR considera outliers apenas valores que estão:")
    print(f"• Abaixo de Q1 - 1.5 × IQR = {lower_bound:.2f}h")
    print(f"• Acima de Q3 + 1.5 × IQR = {upper_bound:.2f}h")
    print(f"")
    print(f'Os valores "altos" mencionados:')
    questionados = [25.58, 1251.91]  # Cycle times do relatório
    for val in questionados:
        dentro = lower_bound <= val <= upper_bound
        print(f"• {val:.2f}h está {'DENTRO' if dentro else 'FORA'} dos limites")
        if dentro:
            print(f"  → Não é outlier porque {val:.2f} ≤ {upper_bound:.2f}")

    # Verificar dados do JSON
    print(f"\n=== COMPARAÇÃO COM DADOS DO SISTEMA ===")
    robust_stats = data["metrics"]["robust_cycle_stats"]
    print(f"Q1 do sistema: {robust_stats['percentile_75'] - robust_stats['iqr']:.2f}h")
    print(f"Q3 do sistema: {robust_stats['percentile_75']:.2f}h")
    print(f"IQR do sistema: {robust_stats['iqr']:.2f}h")
    print(f"Outliers detectados: {robust_stats['outliers_detected']}")


if __name__ == "__main__":
    analyze_outliers()

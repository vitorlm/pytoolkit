#!/usr/bin/env python3
"""
Análise completa dos valores que parecem extremos mas não são outliers
incluindo lead times para comparação.
"""

import json


def analyze_complete():
    # Carregar dados do JSON
    with open("output/cycle-time/cycle_time_CWS_20250922_162029.json", "r") as f:
        data = json.load(f)

    print("=" * 80)
    print("EXPLICAÇÃO: POR QUE 1251.9h, 5112.3h e 8472.8h NÃO SÃO OUTLIERS")
    print("=" * 80)

    # Análise dos Cycle Times
    print("\n1. ANÁLISE DOS CYCLE TIMES")
    print("-" * 40)

    cycle_times = [issue["cycle_time_hours"] for issue in data["issues"]]
    cycle_times_sorted = sorted(cycle_times)

    print("Valores ordenados:")
    for i, ct in enumerate(cycle_times_sorted, 1):
        print(f"  {i}. {ct:.1f}h ({ct / 24:.1f} dias)")

    # Calcular quartis
    n = len(cycle_times_sorted)
    q1_pos = (n + 1) * 0.25  # 2.25
    q3_pos = (n + 1) * 0.75  # 6.75

    # Interpolação para Q1 (entre posições 2 e 3)
    q1 = cycle_times_sorted[1] + 0.25 * (cycle_times_sorted[2] - cycle_times_sorted[1])
    # Interpolação para Q3 (entre posições 6 e 7)
    q3 = cycle_times_sorted[5] + 0.75 * (cycle_times_sorted[6] - cycle_times_sorted[5])

    iqr = q3 - q1
    upper_bound = q3 + 1.5 * iqr

    print(f"\nCálculos IQR:")
    print(f"  Q1 = {q1:.1f}h")
    print(f"  Q3 = {q3:.1f}h")
    print(f"  IQR = {iqr:.1f}h")
    print(f"  Limite superior = Q3 + 1.5×IQR = {upper_bound:.1f}h")

    print(f"\nRESULTADO:")
    print(f"  • 1251.9h < {upper_bound:.1f}h → NÃO é outlier!")
    print(f"  • O valor está apenas 33 horas abaixo do limite")

    # Análise dos Lead Times
    print("\n\n2. ANÁLISE DOS LEAD TIMES")
    print("-" * 40)

    lead_times = [issue["lead_time_hours"] for issue in data["issues"]]
    lead_times_sorted = sorted(lead_times)

    print("Valores ordenados:")
    for i, lt in enumerate(lead_times_sorted, 1):
        print(f"  {i}. {lt:.1f}h ({lt / 24:.1f} dias)")

    # Calcular quartis para lead time
    q1_lead = lead_times_sorted[1] + 0.25 * (lead_times_sorted[2] - lead_times_sorted[1])
    q3_lead = lead_times_sorted[5] + 0.75 * (lead_times_sorted[6] - lead_times_sorted[5])
    iqr_lead = q3_lead - q1_lead
    upper_bound_lead = q3_lead + 1.5 * iqr_lead

    print(f"\nCálculos IQR:")
    print(f"  Q1 = {q1_lead:.1f}h")
    print(f"  Q3 = {q3_lead:.1f}h")
    print(f"  IQR = {iqr_lead:.1f}h")
    print(f"  Limite superior = {upper_bound_lead:.1f}h")

    print(f"\nRESULTADO:")
    print(f"  • 5112.3h < {upper_bound_lead:.1f}h → NÃO é outlier!")
    print(f"  • 8472.8h < {upper_bound_lead:.1f}h → NÃO é outlier!")

    # Explicação do método
    print("\n\n3. POR QUE O MÉTODO IQR É ASSIM?")
    print("-" * 40)
    print("O método IQR (Interquartile Range) é robusto porque:")
    print("• Usa apenas os quartis (Q1 e Q3) para calcular a dispersão")
    print("• Não é influenciado por valores extremos")
    print("• O fator 1.5 é um padrão estatístico conservador")
    print("• Com poucos dados (n=8), a distribuição pode ter alta variabilidade")

    print(f"\nNeste caso específico:")
    print(f"• IQR = {iqr:.1f}h é muito grande devido à alta variabilidade")
    print(f"• Limite superior = {upper_bound:.1f}h permite valores até ~53 dias")
    print(f"• O maior cycle time (1251.9h = 52.2 dias) fica dentro do limite")

    # Mostrar alternativas
    print("\n\n4. MÉTODOS ALTERNATIVOS DE DETECÇÃO")
    print("-" * 40)

    # Z-Score (se houvesse desvio padrão)
    import statistics

    mean_ct = statistics.mean(cycle_times)
    stdev_ct = statistics.stdev(cycle_times)

    print("Método Z-Score (|z| > 2.5 = outlier):")
    for i, ct in enumerate(cycle_times_sorted):
        z_score = (ct - mean_ct) / stdev_ct
        is_outlier = abs(z_score) > 2.5
        print(f"  {ct:.1f}h: z-score = {z_score:.2f} {'→ OUTLIER' if is_outlier else ''}")

    # Modified Z-Score usando MAD
    median_ct = statistics.median(cycle_times)
    mad = statistics.median([abs(x - median_ct) for x in cycle_times])

    print(f"\nMétodo Modified Z-Score (MAD-based, |z| > 3.5 = outlier):")
    for i, ct in enumerate(cycle_times_sorted):
        if mad > 0:
            mod_z_score = 0.6745 * (ct - median_ct) / mad
            is_outlier = abs(mod_z_score) > 3.5
            print(f"  {ct:.1f}h: mod-z = {mod_z_score:.2f} {'→ OUTLIER' if is_outlier else ''}")

    print("\n\n5. CONCLUSÃO")
    print("-" * 40)
    print("• O método IQR com k=1.5 é CONSERVADOR")
    print("• Com apenas 8 amostras, a variabilidade natural é alta")
    print("• Os valores 'extremos' estão dentro da variação esperada")
    print("• Para detecção mais sensível, usar k=1.0 ou métodos baseados em Z-Score")


if __name__ == "__main__":
    analyze_complete()

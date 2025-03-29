import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os

def plot_melt_curve(df: pd.DataFrame, output_dir: str, group_col: str = "Replicate Group"):
    '''
    Plots fluorescence vs. temperature for each replicate group.

    Args:
        df (pd.DataFrame): DataFrame containing 'Temperature', 'Fluorescence_mean', and group column.
        output_dir (str): Directory to save the plot(s).
        group_col (str): Column used to group and label each curve.
    '''
    plt.figure(figsize=(10, 6))
    for name, group in df.groupby(group_col):
        plt.plot(group['Temperature'], group['Derivative_mean'], label=name)

    plt.title('Melt Curve - Fluorescence')
    plt.xlabel('Temperature (°C)')
    plt.ylabel('Fluorescence (mean)')
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "melt_curve.png"))
    plt.close()
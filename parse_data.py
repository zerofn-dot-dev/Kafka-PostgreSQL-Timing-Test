import statistics
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

NANO_TO_MILLI = 1_000_000.0

def read_ns_data_as_ms(filepath):
    with open(filepath, 'r') as f:
        return [int(line.strip()) / NANO_TO_MILLI for line in f if line.strip()]

def calculate_stats(data_ms):
    filtered_data = [time for time in data_ms if time <= 3000]

    total_count = len(data_ms)
    # Count how many values were greater than 3000 ms
    count_above_3000 = len(data_ms) - len(filtered_data)
    data_ms = filtered_data

    stats = {}
    stats['count'] = total_count
    stats['mean'] = round(statistics.mean(data_ms), 5)
    stats['median'] = round(statistics.median(data_ms), 5)
    stats['stdev'] = round(statistics.stdev(data_ms), 5) if len(data_ms) > 1 else 0.0
    stats['min'] = round(min(data_ms), 5)
    stats['max'] = round(max(data_ms), 5)
    stats['count_above_3000'] = count_above_3000
    
    # Detect outliers using IQR method
    q1 = np.percentile(data_ms, 25)
    q3 = np.percentile(data_ms, 75)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    stats['outliers'] = [round(x, 5) for x in data_ms if x < lower_bound or x > upper_bound]

    # Plot the data
    data = np.array(data_ms)
    plt.hist(data, bins=50, color='skyblue', edgecolor='black')
    plt.title('Data Distribution (Histogram)')
    plt.xlabel('Time (ms)')
    plt.ylabel('Frequency')
    plt.savefig('data_distribution_histogram.png')
    plt.close()

    # Plot the data with seaborn
    sns.kdeplot(data, shade=True)
    plt.title('Data Distribution (KDE)')
    plt.xlabel('Time (ms)')
    plt.savefig('data_distribution_kde.png') 
    plt.close()  

    # Box plot
    plt.boxplot(data, vert=False)
    plt.title('Data Distribution (Boxplot)')
    plt.xlabel('Time (ms)')
    plt.savefig('boxplot.png') 
    plt.close()

    # Combined plot
    fig, (ax_box, ax_hist) = plt.subplots(2, sharex=True,
                                      gridspec_kw={"height_ratios": (.15, .85)})

    sns.boxplot(data, ax=ax_box, orient='h')
    sns.histplot(data, bins=50, ax=ax_hist, color='skyblue')

    ax_box.set(xlabel='')
    plt.boxplot(data, vert=False)
    plt.title('Data Distribution (Boxplot)')
    plt.xlabel('Time (ms)')
    plt.savefig('combined.png') 
    plt.close()
    
    # log scale
    plt.hist(data, bins=50, color='skyblue', edgecolor='black')
    plt.xscale('log')
    plt.title('Data Distribution (Histogram, Log Scale)')
    plt.xlabel('Time (ms)')
    plt.ylabel('Frequency')
    plt.savefig('log_scale.png') 
    plt.close()
    
    return stats

def print_stats(stats):
    print(f"Count     : {stats['count']}")
    print(f"Mean      : {stats['mean']} ms")
    print(f"Median    : {stats['median']} ms")
    print(f"Stdev     : {stats['stdev']} ms")
    print(f"Min       : {stats['min']} ms")
    print(f"Max       : {stats['max']} ms")
    print(f"Outliers  : {stats['outliers']} ms")
    print(f"Timeouts   : {stats['count_above_3000']}")

def main():
    try:
        data_ms = read_ns_data_as_ms("data.txt")
        if not data_ms:
            print("No data found.")
            return
        stats = calculate_stats(data_ms)
        print_stats(stats)
    except FileNotFoundError:
        print("File 'data.text' not found.")
    except ValueError as e:
        print(f"Error reading data: {e}")

if __name__ == "__main__":
    main()

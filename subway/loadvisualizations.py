def plot_total_trips(cdta_df, pu_do, single_month, year_month, save_png):
    # 1
    total_day_df = cdta_df[['borough'] + [col for col in cdta_df.columns if f"{pu_do}_total_trip_count_day" in col]]\
                    .groupby('borough').sum().T
    total_day_df.index = [idx.split("_")[-1] for idx in total_day_df.index]

    # 2
    total_hour_df = cdta_df[['borough'] + [col for col in cdta_df.columns if f"{pu_do}_total_trip_count_hour" in col]]\
                    .groupby('borough').sum().T
    total_hour_df.index = [idx.split("_")[-1] for idx in total_hour_df.index]

    # 3
    total_weekday_df = cdta_df[['borough'] + 
                              [f'{pu_do}_total_trip_count_weekday_Friday',
                              f'{pu_do}_total_trip_count_weekday_Monday',
                              f'{pu_do}_total_trip_count_weekday_Saturday',
                              f'{pu_do}_total_trip_count_weekday_Sunday',
                              f'{pu_do}_total_trip_count_weekday_Thursday',
                              f'{pu_do}_total_trip_count_weekday_Tuesday',
                              f'{pu_do}_total_trip_count_weekday_Wednesday']]\
                        .groupby('borough').sum().T
    total_weekday_df.index = [idx.split("_")[-1] for idx in total_weekday_df.index]
    weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    total_weekday_df.index = pd.Categorical(total_weekday_df.index, categories=weekdays, ordered=True)
    total_weekday_df.sort_index(inplace=True)
    
    if single_month:
        fig, axs = plt.subplots(3, 2, figsize=(20, 20))
        total_day_df.plot(ax=axs[0, 0])
        axs[0, 0].set_title(f"total_trip_count_day_{year_month}")

        total_hour_df.plot(ax=axs[1, 0])
        axs[1, 0].set_title(f"total_trip_count_hour_{year_month}")

        total_weekday_df.plot(ax=axs[2, 0])
        axs[2, 0].set_title(f"total_trip_count_weekday_{year_month}")
        
        (total_day_df / 6).plot(ax=axs[0, 1])
        axs[0, 1].set_title(f"monthly_average_count_day_{year_month}")

        (total_hour_df / 6).plot(ax=axs[1, 1])
        axs[1, 1].set_title(f"monthly_average_count_hour_{year_month}")

        (total_weekday_df / 6).plot(ax=axs[2, 1])
        axs[2, 1].set_title(f"monthly_average_count_weekday_{year_month}")
        
    else:
        # 4
        total_year_month_df = cdta_df[['borough'] +
                                      [f'{pu_do}_total_trip_count_year_month_2022-01',
                                       f'{pu_do}_total_trip_count_year_month_2022-02',
                                       f'{pu_do}_total_trip_count_year_month_2022-03',
                                       f'{pu_do}_total_trip_count_year_month_2022-04',
                                       f'{pu_do}_total_trip_count_year_month_2022-05',
                                       f'{pu_do}_total_trip_count_year_month_2022-06']]\
                            .groupby('borough').sum().T
        total_year_month_df.index = [idx.split("_")[-1] for idx in total_year_month_df.index]

        fig, axs = plt.subplots(4, 2, figsize=(20, 20))
        total_day_df.plot(ax=axs[0, 0])
        axs[0, 0].set_title(f"{pu_do}_total_trip_count_day")

        total_hour_df.plot(ax=axs[1, 0])
        axs[1, 0].set_title(f"{pu_do}_total_trip_count_hour")

        total_weekday_df.plot(ax=axs[2, 0])
        axs[2, 0].set_title(f"{pu_do}_total_trip_count_weekday")

        total_year_month_df.plot(ax=axs[3, 0])
        axs[3, 0].set_title(f"{pu_do}_total_trip_count_year_month")
        
        (total_day_df / 6).plot(ax=axs[0, 1])
        axs[0, 1].set_title(f"{pu_do}_monthly_average_count_day")

        (total_hour_df / 6).plot(ax=axs[1, 1])
        axs[1, 1].set_title(f"{pu_do}_monthly_average_count_hour")

        (total_weekday_df / 6).plot(ax=axs[2, 1])
        axs[2, 1].set_title(f"{pu_do}_monthly_average_count_weekday")
        
        bar_df = total_year_month_df.mean().sort_values(ascending=False)
        ax = axs[3, 1]
        ax.bar(bar_df.index, bar_df.values)
        for i, rect in enumerate(ax.patches):
            # Get X and Y placement of label from rect.
            y_value = rect.get_height()
            x_value = rect.get_x() + rect.get_width() / 2

            # Number of points between bar and label
            space = 0
            # Vertical alignment for positive values
            va = 'bottom'

            # If value of bar is negative: Place label below bar
            if y_value < 0:
                # Invert space to place label below
                space *= -1
                # Vertically align label at top
                va = 'top'

            # Use Y value as label and format number with one decimal place
            label = "{:.3f}%".format(bar_df.values[i] / bar_df.values.sum())

            # Create annotation
            ax.annotate(
                label,                      # Use `label` as label
                (x_value, y_value),         # Place label at end of the bar
                xytext=(0, space),          # Vertically shift label by `space`
                textcoords="offset points", # Interpret `xytext` as offset in points
                ha='center',                # Horizontally center label
                va=va)               
        ax.set_title(f"{pu_do}_monthly_average_trip_count_per_borough");       
        
    if year_month == None:
        year_month = "Jan-Jun 2022"
    if save_png:
        matplotlib.use('Agg')
        path = os.getcwd() + "\\" + "data\\png"
        if not os.path.exists(path):
            os.makedirs(path)
        filepath = f'{path}/{year_month}.png'
        chart = fig.get_figure()
        chart.savefig(filepath, dpi=300)
        print(f"{year_month}.png saved in {path}.")
        plt.close(fig)
        
        
def plot_on_map_entries(df, cols):
    cols = [
        f'net_entries',
        f'average_entries',
    ]

    fig, axes = plt.subplots(7, 2, figsize=(15,50))
    for i, col in enumerate(cols):
        if i < 7:
            ax = axes[i%7, 0]
        else:
            ax = axes[i%7, 1]
        divider = make_axes_locatable(ax)
        cax = divider.append_axes("bottom", size="5%", pad=0.5)
        vmin, vmax = df[col].min(), df[col].max()
        df[[col, 'geometry']].plot(
            column=col,
            ax=ax,
            cax=cax,
            legend=True,
            legend_kwds={'label': col,
                        'orientation': 'horizontal'},
            vmin=vmin,
            vmax=vmax
        )
        
def plot_on_map_exits(df, cols):
    cols = [
        f'net_exits',
        f'average_exits',
    ]

    fig, axes = plt.subplots(7, 2, figsize=(15,50))
    for i, col in enumerate(cols):
        if i < 7:
            ax = axes[i%7, 0]
        else:
            ax = axes[i%7, 1]
        divider = make_axes_locatable(ax)
        cax = divider.append_axes("bottom", size="5%", pad=0.5)
        vmin, vmax = df[col].min(), df[col].max()
        df[[col, 'geometry']].plot(
            column=col,
            ax=ax,
            cax=cax,
            legend=True,
            legend_kwds={'label': col,
                        'orientation': 'horizontal'},
            vmin=vmin,
            vmax=vmax
        )       

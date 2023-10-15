from eda_1_feature_engineering import meter_usage_engineered as mue, generate_usage_plot
from utils import set_plot_params

colors = set_plot_params() # Enables dark mode, larger canvas sizes, and sizing considerations

# Generate and modify grouped DataFrame
max_mean_dif_df = mue.groupby(['month', 
                               'year', 
                               'meter_id']).agg({'kwh': ['max', 
                                                         'mean', 
                                                         'median'],
                                                 'month_name': 'first'}).reset_index()

max_mean_dif_df['max_mean_diff'] = (((max_mean_dif_df.kwh['max'] - max_mean_dif_df.kwh['mean'])/max_mean_dif_df.kwh['mean']) * 100).round(2)
max_mean_dif_df.columns = max_mean_dif_df.columns.droplevel(level = 1)
max_mean_dif_df.columns = ['month', 'year', 'meter_id', 'max_usage', 'mean_usage', 'median_usage', 'month_name', 'max_mean_diff']
    
plot_params = {
    'max_usage_plot': 
        {'y'    : 'max_usage',
        'title' : 'Max Usage Per 15-Minute Interval By Meter, Month & Year'},
    'mean_usage_plot': {
        'y'     : 'mean_usage',
        'title' : 'Mean Usage Per 15-Minute Interval By Meter, Month & Year'},
    'max_mean_diff_plot': {
        'y'     : 'max_mean_diff',
        'title' : 'Percent Difference Between Mean & Max Usage By Meter & Year'}
}

for k, v in plot_params.items():
    generate_usage_plot(max_mean_dif_df, 
                        'month_name', 
                        v['y'], 
                        'meter_id', 
                        v['title'])

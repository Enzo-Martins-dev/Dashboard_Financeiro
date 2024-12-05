import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
import plotly.express as px
import psycopg2

# Conexão com o banco de dados
connection = psycopg2.connect(
    database="devops-banco",
    user="joaomiguel",
    password="zetagundam",
    host="devops-banco.crkuie6e0fki.us-east-1.rds.amazonaws.com",
    port=5432
)

cursor = connection.cursor()

def fetch_table_data(query, connection):
    cursor = connection.cursor()
    cursor.execute(query)
    records = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    cursor.close()
    return pd.DataFrame(records, columns=columns)

df_users = fetch_table_data("SELECT * FROM users;", connection)
df_finances = fetch_table_data("SELECT * FROM finances;", connection)
df_merchants = fetch_table_data("SELECT * FROM merchants;", connection)
df_transactions = fetch_table_data("SELECT * FROM transactions limit 10000;", connection)

connection.close()

# Limpeza e transformação dos dados
columns_to_clean = ["per_capita_income", "yearly_income", "total_debt"]
for column in columns_to_clean:
    df_finances[column] = df_finances[column].str.replace("$", "").astype(float)

df_finance = pd.DataFrame(df_finances)
df_finance['Income_Group'] = (df_finance['yearly_income'] // 10000) * 10000

# Agrupamento de comerciantes por estado
grouped_merchants = df_merchants.groupby('merchant_state', as_index=False).size()
grouped_merchants.columns = ['merchant_state', 'merchant_count']

# Agrupamento de `use_chip` para o gráfico de pizza
use_chip_counts = df_transactions['use_chip'].value_counts().reset_index()
use_chip_counts.columns = ['use_chip', 'count']

# Iniciando o aplicativo Dash
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Análise de Dívidas e Informações Comerciais", style={'textAlign': 'center'}),
    
    html.H2("Distribuição de Dívidas por Faixa de Renda", style={'textAlign': 'center'}),
    dcc.Graph(id='histogram-plot'),
    html.P("Selecione a métrica de Dívida:"),
    dcc.Dropdown(
        id='metric-dropdown',
        options=[
            {'label': 'Soma de Dívida (Total Debt)', 'value': 'sum'},
            {'label': 'Média de Dívida (Total Debt)', 'value': 'mean'}
        ],
        value='sum',
        clearable=False
    ),

    html.H2("Quantidade de Comerciantes por Estado", style={'textAlign': 'center'}),
    dcc.Graph(
        id='merchants-by-state-plot',
        figure=px.bar(
            grouped_merchants,
            x='merchant_state',
            y='merchant_count',
            title="Quantidade de Comerciantes por Estado",
            labels={'merchant_state': 'Estado', 'merchant_count': 'Quantidade de Comerciantes'},
            template="plotly_white"
        ).update_traces(marker_color='orange', marker_line_width=1.5)
         .update_layout(xaxis=dict(ticks="outside"))
    ),

    html.H2("Distribuição de Uso de Chip em Transações", style={'textAlign': 'center'}),
    dcc.Graph(
        id='use-chip-pie-chart',
        figure=px.pie(
            use_chip_counts,
            names='use_chip',
            values='count',
            title="Distribuição de Transações com Uso de Chip",
            labels={'use_chip': 'Uso de Chip', 'count': 'Quantidade'},
            template="plotly_white"
        ).update_traces(textinfo='percent+label')
    )
])

# Callback para o gráfico de distribuição de dívidas
@app.callback(
    Output('histogram-plot', 'figure'),
    Input('metric-dropdown', 'value')
)
def update_histogram(selected_metric):
    if selected_metric == 'sum':
        grouped_df = df_finance.groupby('Income_Group', as_index=False)['total_debt'].sum()
        title = "Soma de Dívida por Faixa de Renda"
    elif selected_metric == 'mean':
        grouped_df = df_finance.groupby('Income_Group', as_index=False)['total_debt'].mean()
        title = "Média de Dívida por Faixa de Renda"

    fig = px.bar(
        grouped_df,
        x='Income_Group',
        y='total_debt',
        title=title,
        labels={
            'Income_Group': 'Faixa de Renda (Agrupada em 10k)',
            'total_debt': 'Dívida Total'
        },
        template="plotly_white"
    )
    fig.update_traces(marker_color='blue', marker_line_width=1.5)
    fig.update_layout(xaxis=dict(tickprefix="$", ticks="outside"))
    return fig

if __name__ == "__main__":
    app.run_server(debug=True)

import streamlit as st
import requests
import zipfile
import io
import pandas as pd
import os
import gdown
import tempfile

def download_file_from_github(url, save_path):
    response = requests.get(url)
    if response.status_code == 200:
        with open(save_path, 'wb') as file:
            file.write(response.content)
        st.write(f"File downloaded successfully and saved to {save_path}")
    else:
        st.write(f"Failed to download file. Status code: {response.status_code}")

def load_excel(file_path):
    with open(file_path, 'rb') as file:
        model = pd.read_excel(file, engine='openpyxl')
    return model

def process_data(all_cab, bulan):
    with tempfile.TemporaryDirectory() as tmpdirname:
        def download_file_from_google_drive(file_id, dest_path):
            if not os.path.exists(dest_path):
                url = f"https://drive.google.com/uc?id={file_id}"
                gdown.download(url, dest_path, quiet=False)
                with zipfile.ZipFile(dest_path, 'r') as zip_ref:
                    zip_ref.extractall(tmpdirname)
        
        file_id = '1BP3-98cKLKgY3flpsyuhjbE7zXWNSN3V'
        dest_path = f'{tmpdirname}/downloaded_file.zip'
        download_file_from_google_drive(file_id, dest_path)

        directory = f'{tmpdirname}/Merge'
        dfs = []
        for filename in os.listdir(directory):
            if filename.endswith('.csv'):
                filepath = os.path.join(directory, filename)
                try:
                    df = pd.read_csv(filepath)
                    df.columns = [x.strip() for x in df.columns]
                    dfs.append(df)
                except Exception as e:
                    st.write(f"Error reading {filepath}: {e}")
        if dfs:
            df_merge = pd.concat(dfs, ignore_index=True)

        directory = f'{tmpdirname}/Breakdown'
        dfs = []
        for filename in os.listdir(directory):
            if filename.endswith('.csv'):
                filepath = os.path.join(directory, filename)
                try:
                    dfs.append(pd.read_csv(filepath))
                except Exception as e:
                    st.write(f"Error reading {filepath}: {e}")
        if dfs:
            df_breakdown = pd.concat(dfs, ignore_index=True)
        
        df_merge['NOM'] = df_merge['NOM'].fillna(0)
        df_merge['NOM'] = df_merge['NOM'].apply(lambda x: str(x).strip())
        df_merge = df_merge[(df_merge['NOM'] != 'Cek')]
        df_merge['NOM'] = df_merge['NOM'].apply(lambda x: x.strip().replace('Rp', '').replace(',', '') if 'Rp' in str(x) else x)
        df_merge['NOM'] = df_merge['NOM'].apply(lambda x: -int(x.replace('(', '').replace(')', '')) if '(' in str(x) and ')' in str(x) else x)
        df_merge['NOM'] = df_merge['NOM'].apply(lambda x: x.strip().replace(',', '') if ',' in str(x) else x)
        df_merge = df_merge[(df_merge['NOM'] != '-')]
        df_merge['NOM'] = df_merge['NOM'].astype(float)
        
        df_merge['DATE'] = pd.to_datetime(df_merge['DATE'], format='%d/%m/%Y')
        df_breakdown['DATE'] = pd.to_datetime(df_breakdown['DATE'], format='%d/%m/%Y')

        df_merge['MONTH'] = df_merge['DATE'].dt.month_name()
        df_breakdown['MONTH'] = df_breakdown['DATE'].dt.month_name()
        
        df_merge = df_merge[df_merge['MONTH'] == bulan]
        df_breakdown = df_breakdown[df_breakdown['MONTH'] == bulan]
        
        df_merge['KAT'] = df_merge['KAT'].str.upper()

        kat_pengurang = ['Invoice Beda Hari', 'Transaksi Kemarin', 'Selisih IT', 'Promo Marketing/Adjustment', 'Cancel Nota', 'Tidak Ada Transaksi di Web', 'Selisih Lebih Bayar QRIS', 'Selisih Lebih Bayar Ojol', 'Salah Slot Pembayaran']
        kat_diperiksa = ['Tidak Ada Invoice QRIS', 'Tidak Ada Invoice Ojol', 'Double Input', 'Selisih Kurang Bayar QRIS', 'Selisih Kurang Bayar Ojol', 'Bayar Lebih dari 1 Kali - 1 Struk (QRIS)', 'Bayar 1 Kali - Banyak Struk (QRIS)', 'Bayar Lebih dari 1 Kali - Banyak Struk (QRIS)', 'Kurang Input (Ojol)']
        df_breakdown['Kategori'] = df_breakdown['Kategori'].str.upper()

        df_breakdown.columns = df_breakdown.columns[:-7].to_list() + ['GO RESTO', 'GRAB FOOD', 'QRIS SHOPEE', 'QRIS TELKOM/ESB', 'SHOPEEPAY'] + df_breakdown.columns[-2:].to_list()
        df_breakdown.iloc[:, 9:14] = df_breakdown.iloc[:, 9:14].applymap(lambda x: str(x).replace(',', '')).astype('float')
        
        for cab in all_cab:
            df_merge2 = df_merge[df_merge['CAB'] == cab]
            df_breakdown2 = df_breakdown[df_breakdown['CAB'] == cab]
                
            df_merge2 = df_merge2.groupby(['SOURCE', 'KAT'])[['NOM']].sum().reset_index()
            for i in ['GO RESTO', 'GRAB FOOD', 'QRIS SHOPEE', 'SHOPEEPAY']:
                if i not in df_merge2['KAT'].values:
                    df_merge2.loc[len(df_merge2)] = ['INVOICE', i, 0]
                    df_merge2.loc[len(df_merge2)] = ['WEB', i, 0]
                
            df_merge3 = df_merge2[df_merge2['KAT'].isin(['QRIS ESB', 'QRIS TELKOM'])].groupby('SOURCE')[['NOM']].sum().reset_index()
            df_merge3['KAT'] = 'QRIS TELKOM/ESB'
            
            if df_merge3.empty:
                df_merge3.loc[len(df_merge3)] = ['INVOICE', 0, 'QRIS TELKOM/ESB']
                df_merge3.loc[len(df_merge3)] = ['WEB', 0, 'QRIS TELKOM/ESB']
    
            df_merge_final = pd.pivot(data=pd.concat([df_merge2[df_merge2['KAT'].isin(['GO RESTO', 'GRAB FOOD', 'QRIS SHOPEE', 'SHOPEEPAY'])], df_merge3]), 
                     index='SOURCE', columns='KAT', values='NOM')
            df_merge_final = df_merge_final.reset_index().fillna(0)
            df_merge_final.loc[len(df_merge_final)] = ['SELISIH',
                                           df_merge_final.iloc[0, 1] - df_merge_final.iloc[1, 1],
                                           df_merge_final.iloc[0, 2] - df_merge_final.iloc[1, 2],
                                           df_merge_final.iloc[0, 3] - df_merge_final.iloc[1, 3],
                                           df_merge_final.iloc[0, 4] - df_merge_final.iloc[1, 4],
                                           df_merge_final.iloc[0, 5] - df_merge_final.iloc[1, 5]]
            
            def highlight_last_row(x):
                font_color = 'color: white;'
                background_color = 'background-color: #FF4B4B;'
                df_styles = pd.DataFrame('', index=x.index, columns=x.columns)
                df_styles.iloc[-1, :] = font_color + background_color
                return df_styles
            
            def format_number(x):
                if isinstance(x, (int, float)):
                    return "{:,.0f}".format(x)
                return x
            
            df_merge_final = df_merge_final.applymap(format_number)
            st.markdown(f'## {cab}')
            st.markdown('#### SELISIH PER-PAYMENT')
            df_merge_final = df_merge_final.style.apply(highlight_last_row, axis=None)
            st.dataframe(df_merge_final, use_container_width=True, hide_index=True)
            
            st.markdown('#### KATEGORI PENGURANG')
            df_breakdown_pengurang = df_breakdown2[df_breakdown2['Kategori'].isin([x.upper() for x in kat_pengurang])].groupby('Kategori')[df_breakdown.columns[-7:-2]].sum().reset_index()
            df_breakdown_pengurang.loc[len(df_breakdown_pengurang)] = ['TOTAL',
                                                                      df_breakdown_pengurang.iloc[:, 1].sum(),
                                                                      df_breakdown_pengurang.iloc[:, 2].sum(),
                                                                      df_breakdown_pengurang.iloc[:, 3].sum(),
                                                                      df_breakdown_pengurang.iloc[:, 4].sum(),
                                                                      df_breakdown_pengurang.iloc[:, 5].sum()]
            df_breakdown_pengurang = df_breakdown_pengurang.applymap(format_number)
            df_breakdown_pengurang = df_breakdown_pengurang.style.apply(highlight_last_row, axis=None)
            st.dataframe(df_breakdown_pengurang, use_container_width=True, hide_index=True)
    
            st.markdown('#### KATEGORI DIPERIKSA')
            df_breakdown_diperiksa = df_breakdown2[df_breakdown2['Kategori'].isin([x.upper() for x in kat_diperiksa])].groupby('Kategori')[df_breakdown.columns[-7:-2]].sum().reset_index()
            df_breakdown_diperiksa.loc[len(df_breakdown_diperiksa)] = ['TOTAL',
                                                                      df_breakdown_diperiksa.iloc[:, 1].sum(),
                                                                      df_breakdown_diperiksa.iloc[:, 2].sum(),
                                                                      df_breakdown_diperiksa.iloc[:, 3].sum(),
                                                                      df_breakdown_diperiksa.iloc[:, 4].sum(),
                                                                      df_breakdown_diperiksa.iloc[:, 5].sum()]
            df_breakdown_diperiksa = df_breakdown_diperiksa.applymap(format_number)
            df_breakdown_diperiksa = df_breakdown_diperiksa.style.apply(highlight_last_row, axis=None)
            st.dataframe(df_breakdown_diperiksa, use_container_width=True, hide_index=True)
            st.markdown('---')

# Main Streamlit app code
st.title('Dashboard - Selisih Ojol')

col = st.columns(2)

with col[0]:
    all_cab = st.multiselect('Pilih Cabang', [])
    all_cab = list(all_cab)

with col[1]:
    all_bulan = [
        'January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December'
    ]
    bulan = st.selectbox('Pilih Bulan', all_bulan)

if "button_clicked" not in st.session_state:
    st.session_state.button_clicked = False

def process_callback():
    st.session_state.button_clicked = True

if st.button("Process"):
    # Download data
    url = 'https://raw.githubusercontent.com/Analyst-FPnA/Dashboard-OJOL/main/list_cab.xlsx'
    save_path = 'list_cab.xlsx'
    download_file_from_github(url, save_path)

    if os.path.exists(save_path):
        list_cab = load_excel(save_path)
        st.write("File loaded successfully")
        all_cab = list_cab['CAB'].sort_values().unique()
    else:
        st.write("File does not exist")

if st.button("Find") or st.session_state.button_clicked:
    if 'list_cab' not in globals() or list_cab.empty:
        st.write("Please load the list of branches first by clicking 'Process'.")
    else:
        st.session_state.button_clicked = False
        st.cache_data.clear()
        st.cache_resource.clear()
    
        # Process data
        process_data(all_cab, bulan)
        
        st.cache_data.clear()
        st.cache_resource.clear()

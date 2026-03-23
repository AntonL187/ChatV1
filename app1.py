import streamlit as st
import pandas as pd
import openpyxl
import re
import io

class StreamlitDataEngine:
    def __init__(self):
        self.variables = {}
        self.df = None
        self.file_obj = None # On stocke l'objet fichier Streamlit

    def get_param(self, param_str, key):
        try:
            parts = [p.strip() for p in param_str.split(',')]
            for p in parts:
                if p.startswith(key):
                    val = p.split(':', 1)[1].strip()
                    return val.replace('"', '').replace('[', '').replace(']', '')
            return None
        except: return None
      
    def match_col(self, pattern, col_list):
        p_clean = pattern.replace('*', '').lower().strip()
        for col in col_list:
            col_str = str(col).lower().strip()
            if pattern.endswith('*'):
                if col_str.startswith(p_clean):
                    return col
            else:
                if col_str == p_clean:
                    return col
        return None

    def run_pipeline(self, rules_df, uploaded_file):
        self.file_obj = uploaded_file
        # On trie les règles
        rules = rules_df.sort_values('Ordre')

        for _, row in rules.iterrows():
            action = row['Action']
            cible = row['Cible']
            params = str(row['Paramètres'])
            on_fail = row['Si Échec']
            msg = row['Message / Question']

            try:
                # --- ACTION : FIND_METADATA ---
                if action == "FIND_METADATA":
                    # On charge le workbook depuis la mémoire
                    wb = openpyxl.load_workbook(self.file_obj, data_only=True)
                    ws = wb.active
                    found = False
                    keyword = self.get_param(params, "keyword")
                    for r in range(1, 20):
                        for c in range(1, 10):
                            cell_val = ws.cell(r, c).value
                            if cell_val and keyword in str(cell_val):
                                self.variables[cible] = ws.cell(r, c+1).value
                                found = True; break
                        if found: break
                    if not found: raise Exception("Metadata non trouvée")
                    st.write(f"ℹ️ Métadonnée trouvée ({cible}) : {self.variables[cible]}")

                # --- ACTION : VALIDATION ---
                elif action == "VALIDATE_MANDATORY_COLUMNS":
                    required = [c.strip() for c in self.get_param(params, "Required").split(';')]
                    preview = pd.read_excel(self.file_obj, header=None, nrows=50).astype(str)
                    flat_content = preview.values.flatten()
                    missing = [req for req in required if not any(req.replace('*', '').lower() in cell.lower() for cell in flat_content)]
                    if missing:
                        raise Exception(f"Colonnes manquantes : {missing}")
                    st.success("✅ Structure du fichier validée.")

                # --- ACTION : LOAD_DATA_TABLE ---
                elif action == "LOAD_DATA_TABLE":
                    start_key = self.get_param(params, "Start_At")
                    manda = [c.strip() for c in self.get_param(params, "Mandatory").split(';')]
                    optio = [c.strip() for c in self.get_param(params, "Optional").split(';')]
                    
                    temp_df = pd.read_excel(self.file_obj, header=None, nrows=50)
                    mask = temp_df.apply(lambda r: r.astype(str).str.contains(start_key).any(), axis=1)
                    header_idx = temp_df[mask].index[0]
                    
                    actual_columns = pd.read_excel(self.file_obj, skiprows=header_idx, nrows=0).columns.tolist()
                    columns_to_load = []
                    for m in manda:
                        found = self.match_col(m, actual_columns)
                        if found: columns_to_load.append(found)
                        else: raise Exception(f"Colonne obligatoire '{m}' absente.")
                    for o in optio:
                        found = self.match_col(o, actual_columns)
                        if found: columns_to_load.append(found)
                    
                    self.df = pd.read_excel(self.file_obj, skiprows=header_idx, usecols=columns_to_load)
                    st.write(f"📊 Tableau chargé : {len(self.df)} lignes.")

                # --- ACTION : RENAME ---
                elif action == "RENAME":
                    new_name = self.get_param(params, "To")
                    real_col = self.match_col(cible, self.df.columns)
                    if real_col:
                        self.df.rename(columns={real_col: new_name}, inplace=True)
                    elif on_fail == "STOP":
                        raise Exception(f"Impossible de renommer '{cible}'")

                # --- ACTION : INJECT ---
                elif action == "INJECT":
                    var_name = self.get_param(params, "Value")
                    self.df[cible] = self.variables.get(var_name, "N/A")

                # --- ACTION : FILTER ---
                elif action == "FILTER":
                    cond_raw = self.get_param(params, "Condition")
                    segments = re.split(r'(!=|==|>=|<=|>|<)', cond_raw)
                    protected_cond = ""
                    for seg in segments:
                        seg = seg.strip()
                        if seg in ['!=', '==', '>=', '<=', '>', '<']:
                            protected_cond += f" {seg} "
                        else:
                            if " " in seg and not seg.startswith("`"):
                                protected_cond += f"`{seg}`"
                            else: protected_cond += seg
                    
                    nb_avant = len(self.df)
                    self.df = self.df.query(protected_cond, engine='python')
                    st.info(f"✂️ Filtrage : {nb_avant} -> {len(self.df)} lignes.")

            except Exception as e:
                if on_fail == "STOP":
                    st.error(f"❌ ERREUR CRITIQUE : {msg} ({e})")
                    return None
                st.warning(f"⚠️ {msg} ({e})")
        
        return self.df

# --- INTERFACE STREAMLIT ---
st.set_page_config(page_title="Data Processor", layout="wide")
st.title("🚀 Smart Data Engine")

col1, col2 = st.columns(2)

with col1:
    st.subheader("1. Configuration")
    rules_file = st.file_uploader("Fichier de Règles (Excel)", type=["xlsx"])

with col2:
    st.subheader("2. Données")
    data_file = st.file_uploader("Fichier Source à traiter", type=["xlsx", "xls"])

if rules_file and data_file:
    if st.button("Lancer le traitement"):
        rules_df = pd.read_excel(rules_file)
        engine = StreamlitDataEngine()
        
        with st.status("Traitement en cours...", expanded=True) as status:
            final_df = engine.run_pipeline(rules_df, data_file)
            status.update(label="Traitement terminé !", state="complete", expanded=False)

        if final_df is not None:
            st.subheader("✅ Résultat")
            st.dataframe(final_df, use_container_width=True)
            
            # Bouton de téléchargement
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                final_df.to_excel(writer, index=False)
            
            st.download_button(
                label="📥 Télécharger le fichier final",
                data=output.getvalue(),
                file_name="Analyse_Prix_Streamlit.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

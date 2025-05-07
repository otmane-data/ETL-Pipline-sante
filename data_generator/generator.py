#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import random
import pandas as pd
import psycopg2
from faker import Faker
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from tqdm import tqdm

# Configuration de Faker pour générer des données en français
fake = Faker(['fr_FR'])
Faker.seed(42)  # Pour la reproductibilité
random.seed(42)

# Configuration de la connexion à la base de données
def get_db_connection():
    """Établit une connexion à la base de données PostgreSQL."""
    try:
        conn = psycopg2.connect(
            host=os.environ.get('DB_HOST', 'localhost'),
            database=os.environ.get('DB_NAME', 'sante_db'),
            user=os.environ.get('DB_USER', 'postgres'),
            password=os.environ.get('DB_PASSWORD', 'postgres'),
            port=os.environ.get('DB_PORT', '5432')
        )
        return conn
    except Exception as e:
        print(f"Erreur de connexion à la base de données: {e}")
        return None

# Générateurs de données pour les dimensions
def generer_dim_temps(date_debut, date_fin):
    """Génère les données pour la dimension temps."""
    temps_data = []
    current_date = date_debut
    temps_id = 1
    
    while current_date <= date_fin:
        jour = current_date.day
        mois = current_date.month
        annee = current_date.year
        trimestre = (mois - 1) // 3 + 1
        semaine_annee = current_date.isocalendar()[1]
        est_weekend = current_date.weekday() >= 5  # 5=samedi, 6=dimanche
        
        temps_data.append({
            'temps_id': temps_id,
            'date': current_date,
            'jour': jour,
            'mois': mois,
            'annee': annee,
            'trimestre': trimestre,
            'semaine_annee': semaine_annee,
            'est_weekend': est_weekend
        })
        
        temps_id += 1
        current_date += timedelta(days=1)
    
    return pd.DataFrame(temps_data)

def generer_dim_patient(nb_patients):
    """Génère les données pour la dimension patient."""
    patient_data = []
    groupes_sanguins = ['A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-']
    
    for i in range(1, nb_patients + 1):
        date_naissance = fake.date_of_birth(minimum_age=1, maximum_age=100)
        age = relativedelta(datetime.now(), date_naissance).years
        sexe = random.choice(['M', 'F'])
        
        patient_data.append({
            'patient_id': i,
            'numero_securite_sociale': fake.numerify('############' + ('1' if sexe == 'M' else '2')),
            'nom': fake.last_name(),
            'prenom': fake.first_name_male() if sexe == 'M' else fake.first_name_female(),
            'date_naissance': date_naissance,
            'age': age,
            'sexe': sexe,
            'groupe_sanguin': random.choice(groupes_sanguins),
            'ville': fake.city(),
            'code_postal': fake.postcode(),
            'pays': 'France'
        })
    
    return pd.DataFrame(patient_data)

def generer_dim_medecin(nb_medecins):
    """Génère les données pour la dimension médecin."""
    medecin_data = []
    specialites = [
        'Médecine générale', 'Cardiologie', 'Dermatologie', 'Gastro-entérologie',
        'Neurologie', 'Ophtalmologie', 'Pédiatrie', 'Psychiatrie', 'Radiologie',
        'Chirurgie', 'Gynécologie', 'Orthopédie', 'ORL', 'Urologie', 'Endocrinologie'
    ]
    
    for i in range(1, nb_medecins + 1):
        medecin_data.append({
            'medecin_id': i,
            'nom': fake.last_name(),
            'prenom': fake.first_name(),
            'specialite': random.choice(specialites),
            'numero_rpps': fake.numerify('##########'),
            'experience_annees': random.randint(1, 40)
        })
    
    return pd.DataFrame(medecin_data)

def generer_dim_etablissement(nb_etablissements):
    """Génère les données pour la dimension établissement."""
    etablissement_data = []
    types_etablissement = [
        'Hôpital public', 'Clinique privée', 'Centre médical', 'EHPAD',
        'Centre de rééducation', 'Maternité', 'Centre psychiatrique'
    ]
    
    for i in range(1, nb_etablissements + 1):
        ville = fake.city()
        etablissement_data.append({
            'etablissement_id': i,
            'nom': f"{random.choice(['Centre Hospitalier', 'Clinique', 'Hôpital', 'Institut'])} {ville}",
            'type': random.choice(types_etablissement),
            'capacite_lits': random.randint(20, 1000),
            'ville': ville,
            'code_postal': fake.postcode(),
            'pays': 'France'
        })
    
    return pd.DataFrame(etablissement_data)

def generer_dim_diagnostic(nb_diagnostics):
    """Génère les données pour la dimension diagnostic."""
    diagnostic_data = []
    
    # Quelques exemples de codes CIM-10 et leurs descriptions
    categories = {
        'A': 'Maladies infectieuses et parasitaires',
        'C': 'Tumeurs malignes',
        'E': 'Maladies endocriniennes, nutritionnelles et métaboliques',
        'F': 'Troubles mentaux et du comportement',
        'G': 'Maladies du système nerveux',
        'I': 'Maladies de l\'appareil circulatoire',
        'J': 'Maladies de l\'appareil respiratoire',
        'K': 'Maladies de l\'appareil digestif',
        'M': 'Maladies du système ostéo-articulaire, des muscles et du tissu conjonctif',
        'R': 'Symptômes, signes et résultats anormaux d\'examens cliniques et de laboratoire'
    }
    
    sous_categories = {
        'A': ['Infections intestinales', 'Tuberculose', 'Infections bactériennes'],
        'C': ['Tumeurs malignes digestives', 'Tumeurs malignes respiratoires', 'Mélanome'],
        'E': ['Diabète', 'Obésité', 'Troubles métaboliques'],
        'F': ['Troubles anxieux', 'Dépression', 'Troubles bipolaires'],
        'G': ['Épilepsie', 'Migraines', 'Parkinson'],
        'I': ['Hypertension', 'Infarctus', 'AVC'],
        'J': ['Pneumonie', 'Asthme', 'BPCO'],
        'K': ['Ulcère gastrique', 'Appendicite', 'Cirrhose'],
        'M': ['Arthrose', 'Lombalgie', 'Ostéoporose'],
        'R': ['Douleur', 'Fièvre', 'Malaise']
    }
    
    for i in range(1, nb_diagnostics + 1):
        categorie_code = random.choice(list(categories.keys()))
        categorie = categories[categorie_code]
        sous_categorie = random.choice(sous_categories[categorie_code])
        code_num = fake.numerify('##.#')
        code_cim10 = f"{categorie_code}{code_num}"
        
        diagnostic_data.append({
            'diagnostic_id': i,
            'code_cim10': code_cim10,
            'description': f"{sous_categorie} - {fake.sentence(nb_words=6)}",
            'categorie': categorie,
            'sous_categorie': sous_categorie
        })
    
    return pd.DataFrame(diagnostic_data)

def generer_dim_medicament(nb_medicaments):
    """Génère les données pour la dimension médicament."""
    medicament_data = []
    
    formes = ['comprimé', 'gélule', 'sirop', 'solution injectable', 'pommade', 'patch', 'spray']
    categories = [
        'Analgésique', 'Antibiotique', 'Antidépresseur', 'Antihypertenseur',
        'Anti-inflammatoire', 'Antihistaminique', 'Anxiolytique', 'Corticoïde',
        'Diurétique', 'Hypnotique', 'Immunosuppresseur', 'Neuroleptique'
    ]
    
    prefixes = ['Abi', 'Acti', 'Allo', 'Bio', 'Cardi', 'Derm', 'Endo', 'Fibro', 'Gastro', 'Hemo', 'Immuno', 'Kine', 'Lipo', 'Myco', 'Neuro', 'Onco', 'Pneumo', 'Reno', 'Thrombo']
    suffixes = ['al', 'ine', 'ol', 'one', 'ium', 'ax', 'ex', 'ix', 'ox', 'um', 'an', 'en', 'on', 'in', 'il']
    
    for i in range(1, nb_medicaments + 1):
        prefix = random.choice(prefixes)
        suffix = random.choice(suffixes)
        nom = f"{prefix}{suffix}"
        forme = random.choice(formes)
        dosage = f"{random.choice([5, 10, 20, 25, 50, 100, 200, 250, 500, 1000])} {random.choice(['mg', 'mcg', 'g', 'ml'])}"
        prix = round(random.uniform(1.5, 150.0), 2)
        
        medicament_data.append({
            'medicament_id': i,
            'nom': nom,
            'forme': forme,
            'dosage': dosage,
            'prix': prix,
            'categorie_therapeutique': random.choice(categories)
        })
    
    return pd.DataFrame(medicament_data)

# Générateurs de données pour les faits
def generer_fact_consultation(df_temps, df_patient, df_medecin, df_etablissement, df_diagnostic, nb_consultations):
    """Génère les données pour la table de faits consultation."""
    consultation_data = []
    
    temps_ids = df_temps['temps_id'].tolist()
    patient_ids = df_patient['patient_id'].tolist()
    medecin_ids = df_medecin['medecin_id'].tolist()
    etablissement_ids = df_etablissement['etablissement_id'].tolist()
    diagnostic_ids = df_diagnostic['diagnostic_id'].tolist()
    
    for i in range(1, nb_consultations + 1):
        temps_id = random.choice(temps_ids)
        patient_id = random.choice(patient_ids)
        medecin_id = random.choice(medecin_ids)
        etablissement_id = random.choice(etablissement_ids)
        diagnostic_id = random.choice(diagnostic_ids) if random.random() > 0.1 else None  # 10% sans diagnostic
        
        consultation_data.append({
            'consultation_id': i,
            'temps_id': temps_id,
            'patient_id': patient_id,
            'medecin_id': medecin_id,
            'etablissement_id': etablissement_id,
            'diagnostic_id': diagnostic_id,
            'duree_minutes': random.randint(10, 120),
            'cout': round(random.uniform(25, 200), 2),
            'urgence': random.random() < 0.2,  # 20% des consultations sont urgentes
            'satisfaction_patient': random.randint(1, 5)
        })
    
    return pd.DataFrame(consultation_data)

def generer_fact_traitement(df_temps, df_patient, df_medecin, df_medicament, df_diagnostic, nb_traitements):
    """Génère les données pour la table de faits traitement."""
    traitement_data = []
    
    temps_ids = df_temps['temps_id'].tolist()
    patient_ids = df_patient['patient_id'].tolist()
    medecin_ids = df_medecin['medecin_id'].tolist()
    medicament_ids = df_medicament['medicament_id'].tolist()
    diagnostic_ids = df_diagnostic['diagnostic_id'].tolist()
    
    for i in range(1, nb_traitements + 1):
        temps_id = random.choice(temps_ids)
        patient_id = random.choice(patient_ids)
        medecin_id = random.choice(medecin_ids)
        medicament_id = random.choice(medicament_ids)
        diagnostic_id = random.choice(diagnostic_ids) if random.random() > 0.1 else None  # 10% sans diagnostic
        
        duree_jours = random.randint(1, 90)  # Entre 1 jour et 3 mois
        prix_medicament = df_medicament.loc[df_medicament['medicament_id'] == medicament_id, 'prix'].values[0]
        cout_total = round(prix_medicament * duree_jours * random.uniform(0.8, 1.2), 2)  # Variation du coût
        
        traitement_data.append({
            'traitement_id': i,
            'temps_id': temps_id,
            'patient_id': patient_id,
            'medecin_id': medecin_id,
            'medicament_id': medicament_id,
            'diagnostic_id': diagnostic_id,
            'duree_jours': duree_jours,
            'cout_total': cout_total,
            'efficacite': random.randint(1, 5),
            'effets_secondaires': random.random() < 0.3  # 30% ont des effets secondaires
        })
    
    return pd.DataFrame(traitement_data)

def generer_fact_analyse(df_temps, df_patient, df_etablissement, nb_analyses):
    """Génère les données pour la table de faits analyse."""
    analyse_data = []
    
    temps_ids = df_temps['temps_id'].tolist()
    patient_ids = df_patient['patient_id'].tolist()
    etablissement_ids = df_etablissement['etablissement_id'].tolist()
    
    types_analyse = [
        'Analyse sanguine', 'Radiographie', 'Scanner', 'IRM', 'Échographie',
        'Électrocardiogramme', 'Test d\'effort', 'Endoscopie', 'Biopsie',
        'Test allergologique', 'Analyse d\'urine', 'Spirométrie'
    ]
    
    for i in range(1, nb_analyses + 1):
        temps_id = random.choice(temps_ids)
        patient_id = random.choice(patient_ids)
        etablissement_id = random.choice(etablissement_ids)
        type_analyse = random.choice(types_analyse)
        
        analyse_data.append({
            'analyse_id': i,
            'temps_id': temps_id,
            'patient_id': patient_id,
            'etablissement_id': etablissement_id,
            'type_analyse': type_analyse,
            'resultat_anormal': random.random() < 0.25,  # 25% de résultats anormaux
            'cout': round(random.uniform(20, 1000), 2),
            'delai_resultat_heures': random.randint(1, 168)  # Entre 1 heure et 1 semaine
        })
    
    return pd.DataFrame(analyse_data)

def generer_fact_occupation_etablissement(df_temps, df_etablissement, nb_occupations):
    """Génère les données pour la table de faits occupation d'établissement."""
    occupation_data = []
    
    temps_ids = df_temps['temps_id'].tolist()
    etablissement_ids = df_etablissement['etablissement_id'].tolist()
    
    # Créer un dictionnaire pour suivre les occupations par établissement
    etablissement_capacites = {}
    for _, row in df_etablissement.iterrows():
        etablissement_capacites[row['etablissement_id']] = row['capacite_lits']
    
    # Pour chaque établissement, générer des données d'occupation pour différentes dates
    for etablissement_id in etablissement_ids:
        capacite = etablissement_capacites[etablissement_id]
        
        # Sélectionner des dates aléatoires
        selected_temps_ids = random.sample(temps_ids, min(nb_occupations, len(temps_ids)))
        
        for temps_id in selected_temps_ids:
            taux_occupation = round(random.uniform(0.3, 0.95), 2)  # Entre 30% et 95%
            nombre_admissions = int(capacite * random.uniform(0.05, 0.2))  # 5-20% de la capacité
            nombre_sorties = int(nombre_admissions * random.uniform(0.8, 1.2))  # Variation autour des admissions
            
            occupation_data.append({
                'occupation_id': len(occupation_data) + 1,
                'temps_id': temps_id,
                'etablissement_id': etablissement_id,
                'taux_occupation': taux_occupation,
                'nombre_admissions': nombre_admissions,
                'nombre_sorties': nombre_sorties,
                'duree_moyenne_sejour': round(random.uniform(1, 15), 2)  # Entre 1 et 15 jours
            })
    
    return pd.DataFrame(occupation_data)

# Fonction pour insérer les données dans la base de données
def inserer_donnees(conn, df, table_name):
    """Insère les données d'un DataFrame dans une table PostgreSQL."""
    cursor = conn.cursor()
    
    # Créer la requête d'insertion
    columns = df.columns.tolist()
    placeholders = ', '.join(['%s'] * len(columns))
    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    
    # Insérer les données ligne par ligne
    for _, row in tqdm(df.iterrows(), total=len(df), desc=f"Insertion dans {table_name}"):
        values = [row[col] for col in columns]
        try:
            cursor.execute(query, values)
        except Exception as e:
            print(f"Erreur lors de l'insertion dans {table_name}: {e}")
            conn.rollback()
            return False
    
    conn.commit()
    return True

# Fonction principale
def main():
    # Paramètres de génération
    date_debut = datetime(2020, 1, 1)
    date_fin = datetime(2023, 12, 31)
    nb_patients = 1000
    nb_medecins = 100
    nb_etablissements = 50
    nb_diagnostics = 200
    nb_medicaments = 150
    nb_consultations = 5000
    nb_traitements = 3000
    nb_analyses = 4000
    nb_occupations_par_etablissement = 100
    
    print("Génération des données de santé...")
    
    # Générer les dimensions
    print("Génération des dimensions...")
    df_temps = generer_dim_temps(date_debut, date_fin)
    df_patient = generer_dim_patient(nb_patients)
    df_medecin = generer_dim_medecin(nb_medecins)
    df_etablissement = generer_dim_etablissement(nb_etablissements)
    df_diagnostic = generer_dim_diagnostic(nb_diagnostics)
    df_medicament = generer_dim_medicament(nb_medicaments)
    
    # Générer les faits
    print("Génération des faits...")
    df_consultation = generer_fact_consultation(df_temps, df_patient, df_medecin, df_etablissement, df_diagnostic, nb_consultations)
    df_traitement = generer_fact_traitement(df_temps, df_patient, df_medecin, df_medicament, df_diagnostic, nb_traitements)
    df_analyse = generer_fact_analyse(df_temps, df_patient, df_etablissement, nb_analyses)
    df_occupation = generer_fact_occupation_etablissement(df_temps, df_etablissement, nb_occupations_par_etablissement)
    
    # Connexion à la base de données
    conn = get_db_connection()
    if conn is None:
        print("Impossible de se connecter à la base de données. Sauvegarde des données en CSV.")
        # Sauvegarder en CSV si pas de connexion à la base de données
        df_temps.to_csv('dim_temps.csv', index=False)
        df_patient.to_csv('dim_patient.csv', index=False)
        df_medecin.to_csv('dim_medecin.csv', index=False)
        df_etablissement.to_csv('dim_etablissement.csv', index=False)
        df_diagnostic.to_csv('dim_diagnostic.csv', index=False)
        df_medicament.to_csv('dim_medicament.csv', index=False)
        df_consultation.to_csv('fact_consultation.csv', index=False)
        df_traitement.to_csv('fact_traitement.csv', index=False)
        df_analyse.to_csv('fact_analyse.csv', index=False)
        df_occupation.to_csv('fact_occupation_etablissement.csv', index=False)
        print("Données sauvegardées en CSV.")
        return
    
    # Insérer les données dans la base de données
    print("Insertion des données dans la base de données...")
    try:
        # Insérer les dimensions d'abord (pour respecter les contraintes de clé étrangère)
        inserer_donnees(conn, df_temps, 'dim_temps')
        inserer_donnees(conn, df_patient, 'dim_patient')
        inserer_donnees(conn, df_medecin, 'dim_medecin')
        inserer_donnees(conn, df_etablissement, 'dim_etablissement')
        inserer_donnees(conn, df_diagnostic, 'dim_diagnostic')
        inserer_donnees(conn, df_medicament, 'dim_medicament')
        
        # Puis insérer les faits
        inserer_donnees(conn, df_consultation, 'fact_consultation')
        inserer_donnees(conn, df_traitement, 'fact_traitement')
        inserer_donnees(conn, df_analyse, 'fact_analyse')
        inserer_donnees(conn, df_occupation, 'fact_occupation_etablissement')
        
        print("Insertion des données terminée avec succès.")
    except Exception as e:
        print(f"Erreur lors de l'insertion des données: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
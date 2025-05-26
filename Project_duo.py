import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError
import atexit # Para asegurar que la conexión a la DB se cierre al salir

# --- Configuración de la página ---
st.set_page_config(
    page_title="Sistema de Gestión de Ventas de Aves",
    page_icon="🐔",
    layout="wide"
)

# --- Configuración y funciones de la base de datos ---
@st.cache_resource
def init_database():
    """
    Inicializa la conexión a la base de datos y crea las tablas si no existen.
    Usa DATABASE_URL del entorno.
    """
    try:
        # Intenta obtener la URL de la base de datos de la variable de entorno
        database_url = os.getenv('DATABASE_URL')
        
        if not database_url:
            st.warning("⚠️ No se encontró la variable de entorno 'DATABASE_URL'. La aplicación funcionará sin persistencia de datos (los datos se perderán al cerrar).")
            return None
        
        # Se añaden parámetros para timeout si no están ya en la URL, útiles para algunas DBs
        if "postgresql" in database_url and "connect_timeout" not in database_url:
            database_url += "?connect_timeout=10"
        
        engine = create_engine(database_url)
        
        # Intentar una conexión para verificar que el engine es válido
        with engine.connect() as conn:
            # Crear las tablas si no existen
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS ventas (
                    id SERIAL PRIMARY KEY,
                    fecha DATE NOT NULL,
                    cliente VARCHAR(100) NOT NULL,
                    tipo VARCHAR(50) NOT NULL,
                    cantidad INTEGER NOT NULL,
                    libras DECIMAL(10,2) NOT NULL,
                    descuento DECIMAL(10,2) NOT NULL,
                    libras_netas DECIMAL(10,2) NOT NULL,
                    precio DECIMAL(10,2) NOT NULL,
                    total_a_cobrar DECIMAL(10,2) NOT NULL,
                    pago_cliente DECIMAL(10,2) NOT NULL,
                    saldo DECIMAL(10,2) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS gastos (
                    id SERIAL PRIMARY KEY,
                    fecha DATE NOT NULL,
                    calculo DECIMAL(10,2) NOT NULL,
                    descripcion TEXT,
                    gasto VARCHAR(100) NOT NULL,
                    dinero DECIMAL(10,2) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            conn.commit()
        
        # Asegurar que el engine se cierre al salir de la aplicación
        atexit.register(lambda: engine.dispose())
        
        st.success("Conexión a la base de datos establecida y tablas verificadas. ¡Listo para operar!")
        return engine
    except OperationalError as e:
        st.error(f"❌ Error de conexión a la base de datos: {e}. Por favor, verifica la URL de la base de datos y que el servidor esté activo. (Detalles: {e})")
        return None
    except SQLAlchemyError as e:
        st.error(f"❌ Error de SQLAlchemy al inicializar la base de datos: {e}. Esto puede ser un problema con la configuración de la DB o el driver. (Detalles: {e})")
        return None
    except Exception as e:
        st.error(f"❌ Un error inesperado ocurrió durante la inicialización de la base de datos: {e}")
        return None

def cargar_ventas_desde_db(engine):
    """Carga todas las ventas desde la base de datos."""
    if not engine: return pd.DataFrame()
    try:
        query = """
        SELECT fecha, cliente, tipo, cantidad, libras, descuento, 
               libras_netas, precio, total_a_cobrar, pago_cliente, saldo
        FROM ventas 
        ORDER BY fecha DESC, id DESC
        """
        return pd.read_sql(query, engine)
    except Exception as e:
        st.error(f"Error cargando ventas desde la base de datos: {e}")
        return pd.DataFrame()

def cargar_gastos_desde_db(engine):
    """Carga todos los gastos desde la base de datos."""
    if not engine: return pd.DataFrame()
    try:
        query = """
        SELECT fecha, calculo, descripcion, gasto, dinero
        FROM gastos 
        ORDER BY fecha DESC, id DESC
        """
        return pd.read_sql(query, engine)
    except Exception as e:
        st.error(f"Error cargando gastos desde la base de datos: {e}")
        return pd.DataFrame()

def guardar_venta_en_db(engine, venta_data):
    """Guarda una nueva venta en la base de datos."""
    if not engine: return False
    try:
        with engine.connect() as conn:
            query = text("""
                INSERT INTO ventas (fecha, cliente, tipo, cantidad, libras, descuento, 
                                  libras_netas, precio, total_a_cobrar, pago_cliente, saldo)
                VALUES (:fecha, :cliente, :tipo, :cantidad, :libras, :descuento,
                        :libras_netas, :precio, :total_a_cobrar, :pago_cliente, :saldo)
            """)
            conn.execute(query, venta_data)
            conn.commit()
        return True
    except Exception as e:
        st.error(f"Error guardando venta en la base de datos: {e}")
        return False

def guardar_gasto_en_db(engine, gasto_data):
    """Guarda un nuevo gasto en la base de datos."""
    if not engine: return False
    try:
        with engine.connect() as conn:
            query = text("""
                INSERT INTO gastos (fecha, calculo, descripcion, gasto, dinero)
                VALUES (:fecha, :calculo, :descripcion, :gasto, :dinero)
            """)
            conn.execute(query, gasto_data)
            conn.commit()
        return True
    except Exception as e:
        st.error(f"Error guardando gasto en la base de datos: {e}")
        return False

def limpiar_ventas_db(engine):
    """Elimina todas las ventas de la base de datos."""
    if not engine: return False
    try:
        with engine.connect() as conn:
            conn.execute(text("DELETE FROM ventas"))
            conn.commit()
        return True
    except Exception as e:
        st.error(f"Error al limpiar ventas de la base de datos: {e}")
        return False

def limpiar_gastos_db(engine):
    """Elimina todos los gastos de la base de datos."""
    if not engine: return False
    try:
        with engine.connect() as conn:
            conn.execute(text("DELETE FROM gastos"))
            conn.commit()
        return True
    except Exception as e:
        st.error(f"Error al limpiar gastos de la base de datos: {e}")
        return False

# --- Funciones auxiliares para DataFrames ---
def get_ventas_df_processed(engine):
    """Carga y procesa el DataFrame de ventas para su visualización."""
    df = cargar_ventas_desde_db(engine)
    if not df.empty:
        # Convertir a formato de fecha localizable para visualización
        df['Fecha'] = pd.to_datetime(df['fecha']).dt.date
        df = df.rename(columns={
            'fecha': 'Fecha DB', 'cliente': 'Cliente', 'tipo': 'Tipo', 'cantidad': 'Cantidad',
            'libras': 'Libras', 'descuento': 'Descuento', 'libras_netas': 'Libras_netas',
            'precio': 'Precio', 'total_a_cobrar': 'Total_a_cobrar', 'pago_cliente': 'Pago_Cliente',
            'saldo': 'Saldo'
        })
        # Ordenar por fecha y luego por cliente para consistencia
        df = df.sort_values(by=['Fecha', 'Cliente'], ascending=[False, True])
    return df

def get_gastos_df_processed(engine):
    """Carga y procesa el DataFrame de gastos para su visualización."""
    df = cargar_gastos_desde_db(engine)
    if not df.empty:
        df['Fecha'] = pd.to_datetime(df['fecha']).dt.date
        df = df.rename(columns={
            'fecha': 'Fecha DB', 'calculo': 'Calculo', 'descripcion': 'Descripcion',
            'gasto': 'Gasto', 'dinero': 'Dinero'
        })
        # Ordenar por fecha
        df = df.sort_values(by='Fecha', ascending=False)
    return df

# --- Inicialización principal ---
engine = init_database()

st.title("🐔 Sistema de Gestión de Ventas de Aves")

# Inicializar datos en session state
if 'ventas_data' not in st.session_state:
    st.session_state.ventas_data = get_ventas_df_processed(engine)

if 'gastos_data' not in st.session_state:
    st.session_state.gastos_data = get_gastos_df_processed(engine)

# --- Listas predefinidas ---
CLIENTES = [
    "D. Vicente", "D. Jorge", "D. Quinde", "Sra. Isabel", "Sra. Alba",
    "Sra Yolanda", "Sra Laura Mercado", "D. Segundo", "Legumbrero",
    "Peruana Posorja", "Sra. Sofia", "Sra. Jessica", "Sra Alado de Jessica",
    "Comedor Gordo Posorja", "Sra. Celeste", "Caro negro", "Tienda Isabel Posorja",
    "Carnicero Posorja", "Senel", "D. Jonny", "D. Sra Madelyn", "Lobo Mercado"
]

TIPOS_AVE = ["Pollo", "Gallina"]

CATEGORIAS_GASTO = [
    "G. Alimentación", "G. Transporte", "G. Producción", "G. Salud",
    "G. Educación", "G. Mano de obra", "G. Pérdida", "G. Varios", "Otros Gastos"
]

# --- Funciones de formateo y cálculo ---
def formatear_moneda(valor):
    """Formatea un valor numérico como una cadena de moneda."""
    try:
        return f"${float(valor):,.2f}"
    except (ValueError, TypeError):
        return "$0.00"

def calcular_libras_netas(libras, descuento):
    """Calcula las libras netas."""
    try:
        return round(float(libras) - float(descuento), 2)
    except:
        return 0.0

def calcular_total_cobrar(libras_netas, precio):
    """Calcula el total a cobrar."""
    try:
        return round(float(libras_netas) * float(precio), 2)
    except:
        return 0.0

def calcular_saldo(total_cobrar, pago_cliente):
    """Calcula el saldo pendiente."""
    try:
        return round(float(total_cobrar) - float(pago_cliente), 2)
    except:
        return 0.0

def analizar_alertas_clientes(ventas_df):
    """Analiza el DataFrame de ventas para identificar clientes con alertas."""
    if ventas_df.empty:
        return pd.DataFrame()

    # Asegúrate de trabajar con una copia para evitar SettingWithCopyWarning
    df_temp = ventas_df.copy()
    df_temp['Fecha'] = pd.to_datetime(df_temp['Fecha'])

    alertas = []

    for cliente in df_temp['Cliente'].unique():
        cliente_ventas = df_temp[df_temp['Cliente'] == cliente].copy()
        cliente_ventas = cliente_ventas.sort_values('Fecha')

        saldo_total = cliente_ventas['Saldo'].sum()

        debe_mas_10 = saldo_total > 10

        dias_consecutivos = 0
        # Filtrar solo fechas con saldo positivo
        fechas_con_saldo = cliente_ventas[cliente_ventas['Saldo'] > 0]['Fecha'].dt.date.unique()

        if len(fechas_con_saldo) >= 2:
            fechas_ordenadas = sorted(list(fechas_con_saldo))
            consecutivos_actual = 1
            max_consecutivos = 1

            for i in range(1, len(fechas_ordenadas)):
                if (fechas_ordenadas[i] - fechas_ordenadas[i-1]).days == 1:
                    consecutivos_actual += 1
                    max_consecutivos = max(max_consecutivos, consecutivos_actual)
                else:
                    consecutivos_actual = 1

            dias_consecutivos = max_consecutivos

        if debe_mas_10 or dias_consecutivos >= 2:
            ultima_fecha = cliente_ventas['Fecha'].max().strftime('%Y-%m-%d')

            motivos = []
            if debe_mas_10:
                motivos.append(f"Debe más de $10 (${saldo_total:.2f})")
            if dias_consecutivos >= 2:
                motivos.append(f"Saldo por {dias_consecutivos} día(s) consecutivo(s)")

            alertas.append({
                'Cliente': cliente,
                'Saldo_Total': saldo_total,
                'Ultima_Venta': ultima_fecha,
                'Motivo_Alerta': " | ".join(motivos),
                'Prioridad': 'Alta' if debe_mas_10 and dias_consecutivos >= 2 else 'Media'
            })

    return pd.DataFrame(alertas)


# --- SECCIÓN 1: TABLA DE VENTAS ---
st.header("📊 Registro de Ventas")

### 🚨 Alertas de Clientes
st.subheader("🚨 Alertas de Clientes")
alertas_df = analizar_alertas_clientes(st.session_state.ventas_data)
if not alertas_df.empty:
    st.dataframe(alertas_df, use_container_width=True, hide_index=True)
    st.warning("Revisa a los clientes listados para gestionar sus saldos.")
else:
    st.info("🎉 ¡No hay alertas de clientes pendientes! Todos los saldos al día.")

st.divider()

### ➕ Agregar Nueva Venta
with st.expander("📝 Formulario de Nueva Venta", expanded=True):
    col1, col2, col3, col4 = st.columns(4)
    
    # Inicializar valores en session_state para que los campos del formulario puedan resetearse
    if 'cantidad_venta_val' not in st.session_state: st.session_state['cantidad_venta_val'] = 0
    if 'libras_venta_val' not in st.session_state: st.session_state['libras_venta_val'] = 0.0
    if 'descuento_venta_val' not in st.session_state: st.session_state['descuento_venta_val'] = 0.0
    if 'precio_venta_val' not in st.session_state: st.session_state['precio_venta_val'] = 0.0
    if 'pago_venta_val' not in st.session_state: st.session_state['pago_venta_val'] = 0.0
    if 'cliente_venta_val' not in st.session_state: st.session_state['cliente_venta_val'] = CLIENTES[0]
    if 'tipo_venta_val' not in st.session_state: st.session_state['tipo_venta_val'] = TIPOS_AVE[0]

    with col1:
        fecha_venta = st.date_input("Fecha", value=date.today(), key="fecha_venta")
        cliente = st.selectbox("Cliente", CLIENTES, key="cliente_venta_input", index=CLIENTES.index(st.session_state['cliente_venta_val']))
        tipo_ave = st.selectbox("Tipo", TIPOS_AVE, key="tipo_venta_input", index=TIPOS_AVE.index(st.session_state['tipo_venta_val']))
    
    with col2:
        cantidad = st.number_input("Cantidad", min_value=0, value=st.session_state['cantidad_venta_val'], step=1, key="cantidad_venta_input")
        libras = st.number_input("Libras", min_value=0.0, value=st.session_state['libras_venta_val'], step=0.1, format="%.2f", key="libras_venta_input")
        descuento = st.number_input("Descuento", min_value=0.0, value=st.session_state['descuento_venta_val'], step=0.1, format="%.2f", key="descuento_venta_input")
    
    with col3:
        libras_netas = calcular_libras_netas(libras, descuento)
        st.info(f"**Libras netas:** {libras_netas:.2f}") 
        
        precio = st.number_input("Precio ($)", min_value=0.0, value=st.session_state['precio_venta_val'], step=0.01, format="%.2f", key="precio_venta_input")
    
    with col4:
        total_cobrar = calcular_total_cobrar(libras_netas, precio)
        st.info(f"**Total a cobrar:** {formatear_moneda(total_cobrar)}")
        
        pago_cliente = st.number_input("Pago - Cliente ($)", min_value=0.0, value=st.session_state['pago_venta_val'], step=0.01, format="%.2f", key="pago_venta_input")
        
        saldo = calcular_saldo(total_cobrar, pago_cliente)
        st.info(f"**Saldo:** {formatear_moneda(saldo)}")
    
    if st.button("💾 Agregar Venta", type="primary", use_container_width=True):
        if cantidad > 0 and libras > 0 and precio > 0:
            venta_data = {
                'fecha': fecha_venta, 'cliente': cliente, 'tipo': tipo_ave,
                'cantidad': cantidad, 'libras': libras, 'descuento': descuento,
                'libras_netas': libras_netas, 'precio': precio,
                'total_a_cobrar': total_cobrar, 'pago_cliente': pago_cliente, 'saldo': saldo
            }
            
            if engine and guardar_venta_en_db(engine, venta_data):
                st.session_state.ventas_data = get_ventas_df_processed(engine)
                st.success(f"✅ Venta para **'{cliente}'** guardada exitosamente en la base de datos.")
            else:
                # Fallback para guardar en memoria si no hay DB o falla
                nueva_fila_display = {
                    'Fecha': fecha_venta, 'Cliente': cliente, 'Tipo': tipo_ave,
                    'Cantidad': cantidad, 'Libras': libras, 'Descuento': descuento,
                    'Libras_netas': libras_netas, 'Precio': precio,
                    'Total_a_cobrar': total_cobrar, 'Pago_Cliente': pago_cliente, 'Saldo': saldo
                }
                # Concatena el nuevo DataFrame al inicio para que aparezca primero
                st.session_state.ventas_data = pd.concat([pd.DataFrame([nueva_fila_display]), st.session_state.ventas_data], ignore_index=True)
                st.success(f"✅ Venta para **'{cliente}'** agregada exitosamente (sin persistencia en DB).")
            
            # Resetear valores del formulario usando keys de session_state
            st.session_state['cantidad_venta_val'] = 0
            st.session_state['libras_venta_val'] = 0.0
            st.session_state['descuento_venta_val'] = 0.0
            st.session_state['precio_venta_val'] = 0.0
            st.session_state['pago_venta_val'] = 0.0
            st.session_state['cliente_venta_val'] = CLIENTES[0] 
            st.session_state['tipo_venta_val'] = TIPOS_AVE[0]   
            
            st.rerun() # Recarga la página para mostrar los cambios y resetear el formulario
        else:
            st.error("❌ Por favor complete los campos obligatorios: **Cantidad**, **Libras**, **Precio**.")

st.divider()

### 📋 Historial de Ventas
if not st.session_state.ventas_data.empty:
    st.subheader("📋 Historial de Ventas")
    df_display = st.session_state.ventas_data.copy()
    # Eliminar columna 'Fecha DB' ya que 'Fecha' es la que se muestra
    df_display = df_display.drop(columns=['Fecha DB'], errors='ignore') 
    
    df_display['Precio'] = df_display['Precio'].apply(formatear_moneda)
    df_display['Total_a_cobrar'] = df_display['Total_a_cobrar'].apply(formatear_moneda)
    df_display['Pago_Cliente'] = df_display['Pago_Cliente'].apply(formatear_moneda)
    df_display['Saldo'] = df_display['Saldo'].apply(formatear_moneda)
    
    st.dataframe(df_display, use_container_width=True, hide_index=True)
    
    # Resumen de ventas
    total_ventas = st.session_state.ventas_data['Total_a_cobrar'].sum()
    total_pagos = st.session_state.ventas_data['Pago_Cliente'].sum()
    saldo_pendiente = st.session_state.ventas_data['Saldo'].sum()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("💰 Total Ventas", formatear_moneda(total_ventas))
    with col2:
        st.metric("💵 Total Pagos Recibidos", formatear_moneda(total_pagos))
    with col3:
        st.metric("📈 Saldo Pendiente General", formatear_moneda(saldo_pendiente))
else:
    st.info("📝 No hay ventas registradas. ¡Empieza a agregar ventas usando el formulario de arriba!")

# Botón para limpiar datos de ventas
if not st.session_state.ventas_data.empty:
    with st.expander("🗑️ Opciones Avanzadas de Ventas (Eliminar Datos)"):
        st.error("¡Esta acción eliminará PERMANENTEMENTE todas las ventas! Úsala con extrema precaución y solo si estás seguro.")
        # Primer nivel de confirmación
        if st.button("🔴 Eliminar TODAS las Ventas (Paso 1: Confirmar)", type="secondary", use_container_width=True, key="limpiar_ventas_confirm_step1"):
            st.session_state['confirm_delete_ventas'] = True
            st.warning("⚠️ ¡Estás a punto de eliminar todos los datos de ventas! Haz clic en el botón rojo de abajo para confirmar la eliminación permanente.")
        
        # Segundo nivel de confirmación
        if st.session_state.get('confirm_delete_ventas', False):
            if st.button("🚨 CONFIRMAR ELIMINACIÓN PERMANENTE DE VENTAS 🚨", type="danger", use_container_width=True, key="limpiar_ventas_confirm_step2"):
                if engine and limpiar_ventas_db(engine):
                    st.session_state.ventas_data = get_ventas_df_processed(engine)
                    st.success("✅ Todas las ventas han sido eliminadas exitosamente de la base de datos y de la aplicación.")
                else:
                    # Si no hay DB o falló, limpiar solo el estado
                    st.session_state.ventas_data = pd.DataFrame(columns=[
                        'Fecha', 'Cliente', 'Tipo', 'Cantidad', 'Libras', 'Descuento', 
                        'Libras_netas', 'Precio', 'Total_a_cobrar', 'Pago_Cliente', 'Saldo'
                    ])
                    st.info("✅ Todas las ventas han sido eliminadas del registro local (sin persistencia en DB).")
                st.session_state['confirm_delete_ventas'] = False # Resetear confirmación
                st.rerun()
            if st.button("Cancelar Eliminación de Ventas", use_container_width=True, key="cancel_delete_ventas_form"):
                st.session_state['confirm_delete_ventas'] = False
                st.info("Operación de limpieza de ventas cancelada.")
                st.rerun()

st.divider()

# --- SECCIÓN 2: TABLA DE GASTOS ---
st.header("💸 Control de Gastos")

### ➕ Agregar Nuevo Gasto
with st.expander("📝 Formulario de Nuevo Gasto", expanded=True):
    col1, col2, col3 = st.columns(3)
    
    # Inicializar valores en session_state
    if 'calculo_gasto_val' not in st.session_state: st.session_state['calculo_gasto_val'] = 0.0
    if 'descripcion_gasto_val' not in st.session_state: st.session_state['descripcion_gasto_val'] = ''
    if 'dinero_gasto_val' not in st.session_state: st.session_state['dinero_gasto_val'] = 0.0
    if 'categoria_gasto_val' not in st.session_state: st.session_state['categoria_gasto_val'] = CATEGORIAS_GASTO[0]

    with col1:
        fecha_gasto = st.date_input("Fecha", value=date.today(), key="fecha_gasto")
        calculo = st.number_input("Cálculo (Opcional)", value=st.session_state['calculo_gasto_val'], step=0.01, format="%.2f", key="calculo_gasto_input")
    
    with col2:
        descripcion = st.text_input("Descripción (Detalle del gasto)", value=st.session_state['descripcion_gasto_val'], key="descripcion_gasto_input")
        categoria_gasto = st.selectbox("Categoría de Gasto", CATEGORIAS_GASTO, key="categoria_gasto_input", index=CATEGORIAS_GASTO.index(st.session_state['categoria_gasto_val']))
    
    with col3:
        dinero = st.number_input("Dinero ($) (Monto del gasto)", min_value=0.0, value=st.session_state['dinero_gasto_val'], step=0.01, format="%.2f", key="dinero_gasto_input")
    
    if st.button("💾 Agregar Gasto", type="primary", use_container_width=True):
        if dinero > 0:
            gasto_data = {
                'fecha': fecha_gasto,
                'calculo': calculo,
                'descripcion': descripcion,
                'gasto': categoria_gasto,
                'dinero': dinero
            }
            
            if engine and guardar_gasto_en_db(engine, gasto_data):
                st.session_state.gastos_data = get_gastos_df_processed(engine)
                st.success(f"✅ Gasto de **'{categoria_gasto}'** por {formatear_moneda(dinero)} guardado exitosamente en la base de datos.")
            else:
                # Fallback para guardar en memoria si no hay DB o falla
                nuevo_gasto_display = {
                    'Fecha': fecha_gasto, 'Calculo': calculo, 'Descripcion': descripcion,
                    'Gasto': categoria_gasto, 'Dinero': dinero
                }
                # Concatena el nuevo DataFrame al inicio para que aparezca primero
                st.session_state.gastos_data = pd.concat([pd.DataFrame([nuevo_gasto_display]), st.session_state.gastos_data], ignore_index=True)
                st.info(f"✅ Gasto de **'{categoria_gasto}'** por {formatear_moneda(dinero)} agregado exitosamente (sin persistencia en DB).")
            
            # Resetear valores del formulario
            st.session_state['calculo_gasto_val'] = 0.0
            st.session_state['descripcion_gasto_val'] = ""
            st.session_state['dinero_gasto_val'] = 0.0
            st.session_state['categoria_gasto_val'] = CATEGORIAS_GASTO[0]
            
            st.rerun()
        else:
            st.error("❌ Por favor, ingrese un valor de **Dinero** mayor a 0 para el gasto.")

st.divider()

### 📈 Historial de Gastos
if not st.session_state.gastos_data.empty:
    st.subheader("📈 Historial de Gastos")
    df_display_gastos = st.session_state.gastos_data.copy()
    # Eliminar columna 'Fecha DB'
    df_display_gastos = df_display_gastos.drop(columns=['Fecha DB'], errors='ignore')
    
    df_display_gastos['Calculo'] = df_display_gastos['Calculo'].apply(formatear_moneda)
    df_display_gastos['Dinero'] = df_display_gastos['Dinero'].apply(formatear_moneda)
    
    st.dataframe(df_display_gastos, use_container_width=True, hide_index=True)
    
    # Resumen de gastos
    total_gastos = st.session_state.gastos_data['Dinero'].sum()
    st.metric("💸 Total Gastos Registrados", formatear_moneda(total_gastos))
else:
    st.info("📝 No hay gastos registrados. ¡Empieza a agregar gastos usando el formulario de arriba!")

# Botón para limpiar datos de gastos
if not st.session_state.gastos_data.empty:
    with st.expander("🗑️ Opciones Avanzadas de Gastos (Eliminar Datos)"):
        st.error("¡Esta acción eliminará PERMANENTEMENTE todos los gastos! Úsala con extrema precaución y solo si estás seguro.")
        # Primer nivel de confirmación
        if st.button("🔴 Eliminar TODOS los Gastos (Paso 1: Confirmar)", type="secondary", use_container_width=True, key="limpiar_gastos_confirm_step1"):
            st.session_state['confirm_delete_gastos'] = True
            st.warning("⚠️ ¡Estás a punto de eliminar todos los datos de gastos! Haz clic en el botón rojo de abajo para confirmar la eliminación permanente.")
        
        # Segundo nivel de confirmación
        if st.session_state.get('confirm_delete_gastos', False):
            if st.button("🚨 CONFIRMAR ELIMINACIÓN PERMANENTE DE GASTOS 🚨", type="danger", use_container_width=True, key="limpiar_gastos_confirm_step2"):
                if engine and limpiar_gastos_db(engine):
                    st.session_state.gastos_data = get_gastos_df_processed(engine)
                    st.success("✅ Todos los gastos han sido eliminados exitosamente de la base de datos y de la aplicación.")
                else:
                    # Si no hay DB o falló, limpiar solo el estado
                    st.session_state.gastos_data = pd.DataFrame(columns=[
                        'Fecha', 'Calculo', 'Descripcion', 'Gasto', 'Dinero'
                    ])
                    st.info("✅ Todos los gastos han sido eliminados del registro local (sin persistencia en DB).")
                st.session_state['confirm_delete_gastos'] = False # Resetear confirmación
                st.rerun()
            if st.button("Cancelar Eliminación de Gastos", use_container_width=True, key="cancel_delete_gastos_form"):
                st.session_state['confirm_delete_gastos'] = False
                st.info("Operación de limpieza de gastos cancelada.")
                st.rerun()

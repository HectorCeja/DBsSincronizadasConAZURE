/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tarea3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Hector
 */
public class TareaTres {

    public static void main(String[] args) throws ClassNotFoundException, SQLException, Exception {
        sincronizarBases();
        System.out.println("Las base de datos ahora estan sincronizadas");
    }
    
    public static void sincronizarBases() throws ClassNotFoundException, SQLException, Exception{
        Connection conA = TareaTres.conexionLocalBaseA();
        Connection conB = TareaTres.conexionLocalBaseB();
        Connection conC = TareaTres.conexionAzure();
        
        informacionLocalAzure(conC);
        List<VentaDTO> ventasA = obtenerDatosLocales(conA, "baseA");
        List<VentaDTO> ventasB = obtenerDatosLocales(conB, "baseB");
        
        insertarEnAzure(conC, ventasA, "baseA");
        insertarEnAzure(conC, ventasB, "baseB");
        
        actualizarIdRemoto(conA, ventasA);
        actualizarIdRemoto(conB, ventasB);
        
        List<VentaDTO> ventasAzureA = getVentasAzure(conC, "baseA");
        List<VentaDTO> ventasAzureB = getVentasAzure(conC, "baseB");
        
        insertarVentasAzureEnLocal(conA, ventasAzureA );
        insertarVentasAzureEnLocal(conB, ventasAzureB );
        
    }

    public static Connection conexionLocalBaseA() throws ClassNotFoundException, SQLException {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        Connection con = DriverManager.getConnection("jdbc:sqlserver://HectorCeja\\dbo:1433;databaseName=BaseDeDatosA", "hector", "1234567");
        return con;
    }
    
    public static Connection conexionLocalBaseB() throws ClassNotFoundException, SQLException {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        Connection con = DriverManager.getConnection("jdbc:sqlserver://HectorCeja\\dbo:1433;databaseName=BaseDeDatosB", "hector", "1234567");
        return con;
    }
    
        public static Connection conexionLocalBaseC() throws ClassNotFoundException, SQLException {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        Connection con = DriverManager.getConnection("jdbc:sqlserver://HectorCeja\\dbo:1433;databaseName=BaseDeDatosC", "hector", "1234567");
        return con;
    }

    public static Connection conexionAzure() throws ClassNotFoundException, SQLException {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        Connection con = DriverManager.getConnection("jdbc:sqlserver://ceja2.database.windows.net:1433;database=baseC", "hcejaPlant", "JonSnow21");
        return con;
    }

    public static void insertarEnAzure(Connection con ,List<VentaDTO> ventas,String origen)throws Exception{
        PreparedStatement pst = null;
        ResultSet rs = null;
        try{
            for(VentaDTO venta: ventas){
            String query = "INSERT into Ventas(emision,Cliente,estado,Vendedor,Importe,Observaciones,Origen,idOriginal) "
            + "VALUES(?,?,?,?,?,?,?,?); ";
            pst = con.prepareStatement( query, Statement.RETURN_GENERATED_KEYS );
            pst.setDate(1, venta.getEmision());
            pst.setString(2, venta.getCliente());
            pst.setString(3, venta.getEstado());
            pst.setString(4, venta.getVendedor());
            pst.setDouble(5, venta.getImporte());
            pst.setString(6, venta.getObservaciones());
            pst.setString(7, venta.getOrigen());
            pst.setInt(8, venta.getId());
            pst.executeUpdate();
            
            rs = pst.getGeneratedKeys();
                    if (rs.next()) {
                        venta.setIdRemoto(rs.getInt(1));
                        
                        for(PartidaVentaDTO partida : venta.getPartidas()){
                            partida.setVentaId(venta.getIdRemoto());
                        }
                        insertarPartidasVentas(con, venta.getPartidas());
                        
                    }
            } 
            
            con.commit();
        }catch(Exception e){
            System.out.println("Error en el metodo setInformacionLocalToRemoteVentas casuado por: "+e.getMessage());
            con.rollback();
        }finally{
            if(pst != null)
                pst.close();
            if(rs != null)
                rs.close();
        }
    }
   
    public static void insertarPartidasVentas(Connection con ,List<PartidaVentaDTO> partidas)throws Exception{
        PreparedStatement pst = null;
        StringBuilder sql = new StringBuilder();
        try{
            for(PartidaVentaDTO partida : partidas){
            String query = "INSERT into PartidasVenta(VentaId,Insertado,Producto,Descripcion,Cantidad,Precio) "
            + "VALUES(?,?,?,?,?,?); ";
            pst = con.prepareStatement(query);
            pst.setInt(1, partida.getVentaId());
            pst.setDate(2, partida.getInsertado());
            pst.setString(3, partida.getProducto());
            pst.setString(4, partida.getDescripcion());
            pst.setDouble(5, partida.getCantidad());
            pst.setDouble(6, partida.getPrecio());
            if (pst.executeUpdate() > 0) {
                    System.out.println("Se insertaron correctamente las partidas");
                }
            }
            con.commit();
        }catch(Exception e){
            System.out.println("Error en el metodo insertarPartidasVentas casuado por: "+e.getMessage());
            con.rollback();
        }finally{
            if(pst != null)
                pst.close();
        }
    }

    public static List<VentaDTO> obtenerDatosLocales(Connection con ,String origen) throws ClassNotFoundException, SQLException {
        PreparedStatement pst = null;
        ResultSet rs = null;
        PreparedStatement pst2 = null;
        ResultSet rs2 = null;
        List<VentaDTO> lista = new ArrayList();
        try {

            String query = "SELECT "
                    + "v.VentaId, "
                    + "v.Emision, "
                    + "v.Cliente, "
                    + "v.Estado, "
                    + "v.Vendedor, "
                    + "v.Importe, "
                    + "v.Observaciones "
                    + "FROM Ventas v "
                    + "WHERE v.idremoto = 0 ";
            pst = con.prepareStatement(query);
            rs = pst.executeQuery();
           
            while (rs.next()) {
                
                    VentaDTO venta = new VentaDTO();
 
                    venta.setId(rs.getInt("VentaId"));
                    venta.setEmision(rs.getDate("Emision"));
                    venta.setCliente(rs.getString("Cliente"));
                    venta.setEstado(rs.getString("Estado"));
                    venta.setVendedor(rs.getString("Vendedor"));
                    venta.setImporte(rs.getDouble("Importe"));
                    venta.setObservaciones(rs.getString("Observaciones"));
                    venta.setOrigen(origen);
                   
                    String query2 = "SELECT * "
                    + "FROM PartidasVenta "
                    + "WHERE VentaId = ? ";

                    pst2 = con.prepareStatement(query2);
                    pst2.setInt(1, venta.getId());
                    rs2 = pst2.executeQuery();
                    List<PartidaVentaDTO> partidas = new ArrayList<>();
                    
                    while(rs2.next()){
                        PartidaVentaDTO partida = new PartidaVentaDTO();
                        partida.setId(rs2.getInt("id"));
                        partida.setVentaId(rs2.getInt("VentaId"));
                        partida.setProducto(rs2.getString("Producto"));
                        partida.setDescripcion(rs2.getString("Descripcion"));
                        partida.setCantidad(rs2.getDouble("Cantidad"));
                        partida.setInsertado(rs2.getDate("Insertado"));
                        partida.setPrecio(rs2.getDouble("Precio"));
                        partidas.add(partida);
                    }
                    venta.setPartidas(partidas);
                    
                    lista.add(venta);
            }
            
            rs.close();
            pst.close();
            //con.close();

        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
           /* if (con != null) {
                con.close();
            }*/
        }

        return lista;
    }
    
    public static void actualizarIdRemoto(Connection con, List<VentaDTO> ventas)throws Exception{
        PreparedStatement pst = null;
        StringBuilder sql = new StringBuilder();
        try{
            sql.append("UPDATE Ventas set idRemoto = ?, Origen = ? ");
            sql.append("WHERE VentaId = ?  ");
            for(VentaDTO venta : ventas){
                
                pst = con.prepareCall(sql.toString());
                pst.setInt(1, venta.getIdRemoto());
                pst.setString(2, venta.getOrigen());
                pst.setInt(3, venta.getId());
                int n = pst.executeUpdate();
                    if( n > 0 ){
                        System.out.println("Se actualizaron correctamente los registros locales" );
                    }
            }
            con.commit();
        }catch(Exception e){
            System.out.println("Error en el metodo actualizarIdRemoto : "+e.getMessage());
            con.rollback();
        }finally{
            if(pst != null)
                pst.close();
        }
    }
    
    public static List<VentaDTO> getVentasAzure(Connection con,String Origen)throws Exception{
        PreparedStatement pst = null;
        ResultSet rs = null;
        PreparedStatement pst2 = null;
        ResultSet rs2 = null;
        ArrayList<VentaDTO> ventas = new ArrayList<VentaDTO>();
        
        try{
            String query = "SELECT * "
            + "FROM Ventas "
            + "WHERE Origen != ? ";
            pst = con.prepareCall(query);
            pst.setString(1, Origen);
            rs = pst.executeQuery();
            while(rs.next()){
                VentaDTO venta = new VentaDTO();
                venta.setId(rs.getInt("VentaId"));
                venta.setCliente(rs.getString("Cliente"));
                venta.setEmision(rs.getDate("Emision"));
                venta.setEstado(rs.getString("ESTADO"));
                venta.setImporte(rs.getDouble("Importe"));
                venta.setVendedor(rs.getString("Vendedor"));
                venta.setObservaciones(rs.getString("Observaciones"));
                venta.setOrigen(rs.getString("Origen"));
                venta.setIdOriginal(rs.getInt("idOriginal"));
                
                 String query2 = "SELECT * "
                    + "FROM PartidasVenta "
                    + "WHERE VentaId = ? ";

                    pst2 = con.prepareStatement(query2);
                    pst2.setInt(1, venta.getId());
                    rs2 = pst2.executeQuery();
                    List<PartidaVentaDTO> partidas = new ArrayList<>();
                    
                    while(rs2.next()){
                        PartidaVentaDTO partida = new PartidaVentaDTO();
                        partida.setId(rs2.getInt("ID"));
                        partida.setVentaId(rs2.getInt("VentaId"));
                        partida.setProducto(rs2.getString("Producto"));
                        partida.setDescripcion(rs2.getString("Descripcion"));
                        partida.setCantidad(rs2.getDouble("Cantidad"));
                        partida.setInsertado(rs2.getDate("Insertado"));
                        partida.setPrecio(rs2.getDouble("Precio"));
                        partidas.add(partida);
                    }
                    venta.setPartidas(partidas);
                
                ventas.add(venta);
            }
        }catch(Exception e){
            System.out.println("Error en el metodo getVentasAzure casuado por: "+e.getMessage());
        }finally{
            if(pst != null)
                pst.close();
            if(rs != null)
                rs.close();
        }
        return ventas;
    }
    public static void insertarVentasAzureEnLocal(Connection con, List<VentaDTO> ventas)throws Exception{
        PreparedStatement pst = null;
        StringBuilder sql = new StringBuilder();
        ResultSet rs = null;
        try{
            for( VentaDTO venta : ventas){
                sql.delete(0, sql.length());
            String query = "INSERT into Ventas(Emision,Cliente,ESTADO,Vendedor,Importe,Observaciones,Origen,idRemoto) "
            + "VALUES(?,?,?,?,?,?,?,?); ";
            pst = con.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            pst.setDate(1, venta.getEmision());
            pst.setString(2, venta.getCliente());
            pst.setString(3, venta.getEstado());
            pst.setString(4, venta.getVendedor());
            pst.setDouble(5, venta.getImporte());
            pst.setString(6, venta.getObservaciones());
            pst.setString(7, venta.getOrigen());
            pst.setInt(8, venta.getId());
            if (pst.executeUpdate() > 0) {
                    rs = pst.getGeneratedKeys();
                    if (rs.next()) {
                        venta.setIdRemoto(rs.getInt(1));
                        
                        for(PartidaVentaDTO partida : venta.getPartidas()){
                            partida.setVentaId(venta.getIdRemoto());
                        }
                        insertarPartidasVentas(con, venta.getPartidas());
                        
                    }
                }
            }
            con.commit();
        }catch(Exception e){
            System.out.println("Error en el metodo insertarVentasRemotas casuado por: "+e.getMessage());
            con.rollback();
        }finally{
            if(pst != null)
                pst.close();
            if(rs != null)
                rs.close();
        }
    }
    public static void informacionLocalAzure(Connection conC)throws Exception{
        PreparedStatement pst = null;
        StringBuilder sql = new StringBuilder();
        try{
            String query = "UPDATE Ventas set Origen ='localC', idOriginal = VentaId "
            + "FROM Ventas "
            + "WHERE Origen = '' AND idOriginal = 0;";
            pst = conC.prepareCall(query);
            if(pst.executeUpdate()>0)
                System.out.println("Se actualizaron correctamente "+pst.executeUpdate()+" registros en Azure" );
            conC.commit();
        }catch(Exception e){
            System.out.println("Error en el metodo informacionLocalAzure casuado por: "+e.getMessage());
            conC.rollback();
        }finally{
            if(pst != null)
                pst.close();
        }
    }
}

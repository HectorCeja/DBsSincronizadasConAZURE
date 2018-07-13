/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tarea3;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 *
 * @author alexis
 */
public class Metodo {
    public void sincronizarServidores(Connection conA,Connection conB,Connection conC) throws Exception{
        try{
            informacionLocalC(conC);
            ArrayList<VentaDTO> ventasA = getVentasLocales(conA, "localA");
            ArrayList<VentaDTO> ventasB = getVentasLocales(conB, "localB");
            setInformacionLocalToRemoteVentas(conC, ventasA, "localA");
            setInformacionLocalToRemoteVentas(conC, ventasB, "localB");
            actualizarIdRemoto(conA, ventasA);
            actualizarIdRemoto(conB, ventasB);
            ArrayList<VentaDTO> ventasRemotasA = getVentasRemotas(conC, "localA");
            ArrayList<VentaDTO> ventasRemotasB = getVentasRemotas(conC, "localB");
            insertarVentasRemotas(conA, ventasRemotasA);
            insertarVentasRemotas(conB, ventasRemotasB);
        }catch(Exception e){
            System.out.println("Error en el proceso");
        }finally{
            if(conA != null)
                conA.close();
            if(conB != null)
                conB.close();
            if(conC != null)
                conC.close();
        }
    }
    public void informacionLocalC(Connection conC)throws Exception{
        PreparedStatement pst = null;
        StringBuilder sql = new StringBuilder();
        try{
            sql.append("UPDATE Ventas set Origen ='localC', idOriginal = VentaId ");
            sql.append("FROM Ventas ");
            sql.append("WHERE Origen = '' AND idOriginal = 0;");
            pst = conC.prepareCall(sql.toString());
            int n = pst.executeUpdate();
            if(n>0)
                System.out.println("Se actualizaron correctamente "+n+" registros en C" );
            conC.commit();
        }catch(Exception e){
            System.out.println("Error en el metodo informacionLocalC casuado por: "+e.getMessage());
            conC.rollback();
        }finally{
            if(pst != null)
                pst.close();
        }
    }
    public ArrayList<PartidaVentaDTO> getPartidasPorVenta(Connection con,Integer idVenta)throws Exception{
        PreparedStatement pst = null;
        ResultSet rs = null;
        StringBuilder sql = new StringBuilder();
        ArrayList<PartidaVentaDTO> partidas = new ArrayList<PartidaVentaDTO>();
        PartidaVentaDTO obj ;
        try{
            sql.append("Select * ");
            sql.append("FROM PartidasVenta ");
            sql.append("WHERE VentaId=? ;");
            pst = con.prepareCall(sql.toString());
            pst.setInt(1, idVenta);
            rs = pst.executeQuery();
            while(rs.next()){
                obj = new PartidaVentaDTO();
                obj.setId(rs.getInt("ID"));
                obj.setVentaId(rs.getInt("VentaId"));
                obj.setProducto(rs.getString("Producto"));
                obj.setDescripcion(rs.getString("Descripcion"));
                obj.setCantidad(rs.getDouble("Cantidad"));
                obj.setInsertado(rs.getDate("Insertado"));
                obj.setPrecio(rs.getDouble("Precio"));
                partidas.add(obj);
            }
        }catch(Exception e){
            System.out.println("Error en el metodo getPartidasPorVenta casuado por: "+e.getMessage());
        }finally{
            if(pst != null)
                pst.close();
        }
        return partidas;
    }
    public ArrayList<VentaDTO> getVentasLocales(Connection con,String origen)throws Exception{
        PreparedStatement pst = null;
        ResultSet rs = null;
        StringBuilder sql = new StringBuilder();
        ArrayList<VentaDTO> ventas = new ArrayList<VentaDTO>();
        VentaDTO obj ;
        try{
            sql.append("Select * ");
            sql.append("FROM Ventas ");
            sql.append("WHERE idRemoto=0 ;");
            pst = con.prepareCall(sql.toString());
            rs = pst.executeQuery();
            while(rs.next()){
                obj = new VentaDTO();
                obj.setId(rs.getInt("VentaId"));
                obj.setCliente(rs.getString("Cliente"));
                obj.setEmision(rs.getDate("Emision"));
                obj.setEstado(rs.getString("ESTADO"));
                obj.setImporte(rs.getDouble("Importe"));
                obj.setVendedor(rs.getString("Vendedor"));
                obj.setObservaciones(rs.getString("Observaciones"));
                obj.setOrigen(origen);
                obj.setPartidas(getPartidasPorVenta(con, obj.getId()));
                ventas.add(obj);
            }
        }catch(Exception e){
            System.out.println("Error en el metodo getVentasLocales casuado por: "+e.getMessage());
        }finally{
            if(pst != null)
                pst.close();
            if(rs != null)
                rs.close();
        }
        return ventas;
    }
    public void setInformacionLocalToRemoteVentas(Connection con ,ArrayList<VentaDTO> ventas,String origen)throws Exception{
        PreparedStatement pst = null;
        StringBuilder sql = new StringBuilder();
        ResultSet rs = null;
        try{
            for(int i=0; i<ventas.size(); i++){
                sql.delete(0, sql.length());
            sql.append("INSERT into Ventas(Emision,Cliente,ESTADO,Vendedor,Importe,Observaciones,Origen,idOriginal) ");
            sql.append("VALUES(?,?,?,?,?,?,?,?); ");
            pst = con.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);
            pst.setDate(1, ventas.get(i).getEmision());
            pst.setString(2, ventas.get(i).getCliente());
            pst.setString(3, ventas.get(i).getEstado());
            pst.setString(4, ventas.get(i).getVendedor());
            pst.setDouble(5, ventas.get(i).getImporte());
            pst.setString(6, ventas.get(i).getObservaciones());
            pst.setString(7, ventas.get(i).getOrigen());
            pst.setInt(8, ventas.get(i).getId());
            if (pst.executeUpdate() > 0) {
                    rs = pst.getGeneratedKeys();
                    if (rs.next()) {
                        ventas.get(i).setIdRemoto(rs.getInt(1));
                        for(int j=0; j<ventas.get(i).getPartidas().size(); j++){
                            ventas.get(i).getPartidas().get(j).setVentaId(ventas.get(i).getIdRemoto());
                        }
                      //  setInformacionLocalToRemotePartidasVentas(con, ventas.get(i).getPartidas());
                    }
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
    public void actualizarIdRemoto(Connection con,ArrayList<VentaDTO> ventas)throws Exception{
        PreparedStatement pst = null;
        StringBuilder sql = new StringBuilder();
        try{
            for(int i=0; i<ventas.size(); i++){
                sql.delete(0, sql.length());
            sql.append("UPDATE Ventas set idRemoto=?,Origen=? ");
            sql.append("WHERE VentaId=? ; ");
            pst = con.prepareCall(sql.toString());
            pst.setInt(1, ventas.get(i).getIdRemoto());
            pst.setString(2, ventas.get(i).getOrigen());
            pst.setInt(3, ventas.get(i).getId());
            int n = pst.executeUpdate();
            if(n>0)
                System.out.println("Se actualizaron correctamente los registros locales" );
            }
            con.commit();
        }catch(Exception e){
            System.out.println("Error en el metodo actualizarIdRemoto casuado por: "+e.getMessage());
            con.rollback();
        }finally{
            if(pst != null)
                pst.close();
        }
    }
    public ArrayList<VentaDTO> getVentasRemotas(Connection con,String Origen)throws Exception{
        PreparedStatement pst = null;
        ResultSet rs = null;
        StringBuilder sql = new StringBuilder();
        ArrayList<VentaDTO> ventas = new ArrayList<VentaDTO>();
        VentaDTO obj ;
        try{
            sql.append("Select * ");
            sql.append("FROM Ventas ");
            sql.append("WHERE Origen!=? ;");
            pst = con.prepareCall(sql.toString());
            pst.setString(1, Origen);
            rs = pst.executeQuery();
            while(rs.next()){
                obj = new VentaDTO();
                obj.setId(rs.getInt("VentaId"));
                obj.setCliente(rs.getString("Cliente"));
                obj.setEmision(rs.getDate("Emision"));
                obj.setEstado(rs.getString("ESTADO"));
                obj.setImporte(rs.getDouble("Importe"));
                obj.setVendedor(rs.getString("Vendedor"));
                obj.setObservaciones(rs.getString("Observaciones"));
                obj.setOrigen(rs.getString("Origen"));
                obj.setIdOriginal(rs.getInt("idOriginal"));
                obj.setPartidas(getPartidasPorVenta(con, obj.getId()));
                ventas.add(obj);
            }
        }catch(Exception e){
            System.out.println("Error en el metodo getVentasRemotas casuado por: "+e.getMessage());
        }finally{
            if(pst != null)
                pst.close();
            if(rs != null)
                rs.close();
        }
        return ventas;
    }
    public void setInformacionLocalToRemotePartidasVentas(Connection con ,ArrayList<PartidaVentaDTO> partidas)throws Exception{
        PreparedStatement pst = null;
        StringBuilder sql = new StringBuilder();
        try{
            for(int i=0; i<partidas.size(); i++){
            sql.delete(0, sql.length());
            sql.append("INSERT into PartidasVenta(VentaId,Insertado,Producto,Descripcion,Cantidad,Precio) ");
            sql.append("VALUES(?,?,?,?,?,?); ");
            pst = con.prepareStatement(sql.toString());
            pst.setInt(1, partidas.get(i).getVentaId());
            pst.setDate(2, partidas.get(i).getInsertado());
            pst.setString(3, partidas.get(i).getProducto());
            pst.setString(4, partidas.get(i).getDescripcion());
            pst.setDouble(5, partidas.get(i).getCantidad());
            pst.setDouble(6, partidas.get(i).getPrecio());
            if (pst.executeUpdate() > 0) {
                    System.out.println("Se insertaron correctamente las partidas");
                }
            }
            con.commit();
        }catch(Exception e){
            System.out.println("Error en el metodo setInformacionLocalToRemotePartidasVentas casuado por: "+e.getMessage());
            con.rollback();
        }finally{
            if(pst != null)
                pst.close();
        }
    }
    public void insertarVentasRemotas(Connection con,ArrayList<VentaDTO> ventas)throws Exception{
        PreparedStatement pst = null;
        StringBuilder sql = new StringBuilder();
        ResultSet rs = null;
        try{
            for(int i=0; i<ventas.size(); i++){
                sql.delete(0, sql.length());
            sql.append("INSERT into Ventas(Emision,Cliente,ESTADO,Vendedor,Importe,Observaciones,Origen,idRemoto) ");
            sql.append("VALUES(?,?,?,?,?,?,?,?); ");
            pst = con.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);
            pst.setDate(1, ventas.get(i).getEmision());
            pst.setString(2, ventas.get(i).getCliente());
            pst.setString(3, ventas.get(i).getEstado());
            pst.setString(4, ventas.get(i).getVendedor());
            pst.setDouble(5, ventas.get(i).getImporte());
            pst.setString(6, ventas.get(i).getObservaciones());
            pst.setString(7, ventas.get(i).getOrigen());
            pst.setInt(8, ventas.get(i).getId());
            if (pst.executeUpdate() > 0) {
                    rs = pst.getGeneratedKeys();
                    if (rs.next()) {
                        ventas.get(i).setIdOriginal(rs.getInt(1));
                        for(int j=0; j<ventas.get(i).getPartidas().size(); j++){
                            ventas.get(i).getPartidas().get(j).setVentaId(ventas.get(i).getIdOriginal());
                        }
                       // setInformacionLocalToRemotePartidasVentas(con, ventas.get(i).getPartidas());
                        System.out.println("Se acutalizo correctamente el servidor local");
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
}

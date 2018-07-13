/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tarea3;

import java.sql.Date;
import java.util.List;

/**
 *
 * @author Hector
 */
public class VentaDTO {

    private Integer id;

    private Date emision;

    private String cliente;

    private String estado;

    private String vendedor;

    private Double importe;

    private String observaciones;
    
    private String origen;
    
    private Integer idOriginal;
    
    private Integer idRemoto;
    
    private List<PartidaVentaDTO> partidas;

    public Integer getIdRemoto() {
        return idRemoto;
    }

    public void setIdRemoto(Integer idRemoto) {
        this.idRemoto = idRemoto;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getOrigen() {
        return origen;
    }

    public void setOrigen(String origen) {
        this.origen = origen;
    }

    public Integer getIdOriginal() {
        return idOriginal;
    }

    public void setIdOriginal(Integer idOriginal) {
        this.idOriginal = idOriginal;
    }

    public Date getEmision() {
        return emision;
    }

    public void setEmision(Date emision) {
        this.emision = emision;
    }

    public String getCliente() {
        return cliente;
    }

    public void setCliente(String cliente) {
        this.cliente = cliente;
    }

    public String getEstado() {
        return estado;
    }

    public void setEstado(String estado) {
        this.estado = estado;
    }

    public String getVendedor() {
        return vendedor;
    }

    public void setVendedor(String vendedor) {
        this.vendedor = vendedor;
    }

    public Double getImporte() {
        return importe;
    }

    public void setImporte(Double importe) {
        this.importe = importe;
    }

    public String getObservaciones() {
        return observaciones;
    }

    public void setObservaciones(String observaciones) {
        this.observaciones = observaciones;
    }

    public List<PartidaVentaDTO> getPartidas() {
        return partidas;
    }

    public void setPartidas(List<PartidaVentaDTO> partidas) {
        this.partidas = partidas;
    }

}

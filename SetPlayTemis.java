/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package GLDC_Queue;


import NewAlertasPF.*;
import NewAlertas.*;
import java.io.Serializable;

/**
 *
 * @author NMI16991
 */
public class SetPlayTemis implements Serializable {
    
    int ID_USUARIO, DQUEUE;
    int ID_QUEUE,  ID_PRIORIDAD;  
    String    FECHA_REGISTRO, ID_TOKEN,MAX_FECHA_ASGINACION;
    
    int TOTAL_ATENDIENDO;


                
    
    // set play carga de trabajo
    public SetPlayTemis(int ID_USUARIO, int TOTAL_ATENDIENDO, String MAX_FECHA_ASGINACION, int DQUEUE, String FECHA_REGISTRO){
        this.ID_USUARIO=ID_USUARIO;
        this.TOTAL_ATENDIENDO=TOTAL_ATENDIENDO;
        this.MAX_FECHA_ASGINACION=MAX_FECHA_ASGINACION;
        this.DQUEUE=DQUEUE;
        this.FECHA_REGISTRO=FECHA_REGISTRO;
    }
    
    // Set play lista trabajo
    public SetPlayTemis(int ID_QUEUE, String ID_TOKEN, int ID_PRIORIDAD, String FECHA_REGISTRO ){
        this.ID_QUEUE=ID_QUEUE;
        this.ID_TOKEN=ID_TOKEN;
        this.ID_PRIORIDAD=ID_PRIORIDAD;
        this.FECHA_REGISTRO=FECHA_REGISTRO;
       
    }

    
    

    public int getID_USUARIO() {
        return ID_USUARIO;
    }

    public void setID_USUARIO(int ID_USUARIO) {
        this.ID_USUARIO = ID_USUARIO;
    }

    public int getDQUEUE() {
        return DQUEUE;
    }

    public void setDQUEUE(int DQUEUE) {
        this.DQUEUE = DQUEUE;
    }

    public int getID_QUEUE() {
        return ID_QUEUE;
    }

    public void setID_QUEUE(int ID_QUEUE) {
        this.ID_QUEUE = ID_QUEUE;
    }

    public String getID_TOKEN() {
        return ID_TOKEN;
    }

    public void setID_TOKEN(String ID_TOKEN) {
        this.ID_TOKEN = ID_TOKEN;
    }


    
    
    public int getID_PRIORIDAD() {
        return ID_PRIORIDAD;
    }

    public void setID_PRIORIDAD(int ID_PRIORIDAD) {
        this.ID_PRIORIDAD = ID_PRIORIDAD;
    }

    public String getFECHA_REGISTRO() {
        return FECHA_REGISTRO;
    }

    public void setFECHA_REGISTRO(String FECHA_REGISTRO) {
        this.FECHA_REGISTRO = FECHA_REGISTRO;
    }

    public int getTOTAL_ATENDIENDO() {
        return TOTAL_ATENDIENDO;
    }

    public void setTOTAL_ATENDIENDO(int TOTAL_ATENDIENDO) {
        this.TOTAL_ATENDIENDO = TOTAL_ATENDIENDO;
    }

    public String getMAX_FECHA_ASGINACION() {
        return MAX_FECHA_ASGINACION;
    }

    public void setMAX_FECHA_ASGINACION(String MAX_FECHA_ASGINACION) {
        this.MAX_FECHA_ASGINACION = MAX_FECHA_ASGINACION;
    }


    
    
 
}

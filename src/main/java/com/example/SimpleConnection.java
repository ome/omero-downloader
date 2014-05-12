/*
 * To the extent possible under law, the OME developers have waived
 * all copyright and related or neighboring rights to this tutorial code.
 *
 * See the CC0 1.0 Universal license for details:
 *     http://creativecommons.org/publicdomain/zero/1.0/
 */
package com.example;

import omero.api.IAdminPrx;

/**
 * A simple connection to an OMERO server.
 *
 * @author The OME Team
 */
public class SimpleConnection {

    /**
     */
    public static void main(String[] args) throws Exception {
        omero.client client = new omero.client(args);
        client.createSession();
        try {
            IAdminPrx admin = client.getSession().getAdminService();
            System.out.println("Logged in as :" + admin.getEventContext().userName);
        } finally {
            client.__del__();
        }
    }
}

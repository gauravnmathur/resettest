/*-
 * ‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
 * Autonomic Proprietary 1.0
 * ——————————————————————————————————————————————————————————————————————————————
 * Copyright (C) 2018 Autonomic, LLC - All rights reserved
 * ——————————————————————————————————————————————————————————————————————————————
 * Proprietary and confidential.
 * 
 * NOTICE:  All information contained herein is, and remains the property of
 * Autonomic, LLC and its suppliers, if any.  The intellectual and technical
 * concepts contained herein are proprietary to Autonomic, LLC and its suppliers
 * and may be covered by U.S. and Foreign Patents, patents in process, and are
 * protected by trade secret or copyright law. Dissemination of this information
 * or reproduction of this material is strictly forbidden unless prior written
 * permission is obtained from Autonomic, LLC.
 * 
 * Unauthorized copy of this file, via any medium is strictly prohibited.
 * ______________________________________________________________________________
 */
package com.autonomic.iam.client.grpc;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Helpers {
    private static final Logger logger = LoggerFactory.getLogger(Helpers.class);


    static boolean hasStatus(StatusRuntimeException sre, Code code) {
        return sre.getStatus().getCode() == code;
    }

    /**
     * We've observed that in the case when a channel is faulty or dead for unknown reasons, that
     * an UNAVAILABLE exception with an inner IOException is reported.
     * See: https://grpc.io/docs/guides/error.html
     * @param sre
     * @return true if the channel is suspected dead.
     */
    static boolean isChannelDead(StatusRuntimeException sre) {
        boolean dead = hasStatus(sre, Code.UNAVAILABLE) && sre.getCause() instanceof IOException;
        if (dead) {
            logger.trace("reporting dead channel based on exception:", sre);
        }
        return dead;
    }

}

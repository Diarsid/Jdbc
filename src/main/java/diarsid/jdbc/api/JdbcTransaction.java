/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package diarsid.jdbc.api;

import diarsid.support.objects.CommonEnum;

/**
 *
 * @author Diarsid
 */
public interface JdbcTransaction extends AutoCloseable, JdbcOperationsTransactional {

    enum ThenDo implements CommonEnum<ThenDo> {
        PROCEED,
        CLOSE,
        THROW
    }

    enum State implements CommonEnum<State> {

        OPEN(true, true),
        FAILED(false, true),
        CLOSED_COMMITTED(false, false),
        CLOSED_ROLLBACKED(false, false);

        private State(boolean isValid, boolean isOpen) {
            this.isValid = isValid;
            this.isOpen = isOpen;
        }

        private boolean isValid;
        private boolean isOpen;

        public boolean isValid() {
            return isValid;
        }

        public boolean isOpen() {
            return isOpen;
        }

        public boolean isNotValid() {
            return !isValid;
        }

        public boolean isNotOpen() {
            return !isOpen;
        }
    }

    @Override // in order not to throw Exception
    void close();
            
    void commitAndClose();
    
    void rollbackAnd(ThenDo thenDo);

}

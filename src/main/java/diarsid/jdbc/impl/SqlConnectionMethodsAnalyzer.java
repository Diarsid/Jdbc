/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package diarsid.jdbc.impl;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableSet;

/**
 *
 * @author Diarsid
 */
class SqlConnectionMethodsAnalyzer {
    
    private static final Set<String> ignoredMethodsNames;
    private static final Set<String> forbiddenMethodNames;
    
    static {        
        String[] forbidden = {"commit", "rollback", "setAutoCommit"};
        String[] ignore = {"close", "abort"};
        
        ignoredMethodsNames = unmodifiableSet(new HashSet<>(asList(ignore)));
        forbiddenMethodNames = unmodifiableSet(new HashSet<>(asList(forbidden)));
    }
    
    SqlConnectionMethodsAnalyzer() {
    }
    
    private boolean methodReturnsResource(Method method) {
        return method.getReturnType().isAssignableFrom(AutoCloseable.class);
    }
    
    private boolean methodIsForbidden(Method method) {
        return forbiddenMethodNames.contains(method.getName());
    }
    
    private boolean methodShouldBeIgnored(Method method) {
        return ignoredMethodsNames.contains(method.getName());
    }
    
    SqlConnectionMethodType defineTypeOf(Method method) {
        if ( this.methodReturnsResource(method) ) {
            return SqlConnectionMethodType.METHOD_OPENS_RESOURCE;
        } else if ( this.methodIsForbidden(method) ) {
            return SqlConnectionMethodType.METHOD_IS_FORBIDDEN;
        } else if ( this.methodShouldBeIgnored(method) ) {
            return SqlConnectionMethodType.METHOD_INVOCATION_IGNORED;
        } else {
            return SqlConnectionMethodType.OTHER;
        }
    }
}

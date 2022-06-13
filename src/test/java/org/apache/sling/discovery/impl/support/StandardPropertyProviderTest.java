/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sling.discovery.impl.support;

import org.apache.sling.settings.SlingSettingsService;
import org.apache.sling.settings.impl.SlingSettingsServiceImpl;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.osgi.framework.ServiceReference;

import static org.junit.Assert.assertNotNull;

public class StandardPropertyProviderTest {

    private StandardPropertyProvider standardPropertyProvider;

    @Rule
    public final OsgiContext context = new OsgiContext();

    @Before
    public void setup() {
        final SlingSettingsService settings = new SlingSettingsServiceImpl("");
        context.registerService(settings);
        standardPropertyProvider = context.registerInjectActivateService(new StandardPropertyProvider(),
                "dummy.property", "Test");
    }

    @Test
    public void testBindHttpService() {
        ServiceReference reference1 = Mockito.mock(ServiceReference.class);
        Mockito.when(reference1.getProperty("osgi.http.endpoint")).thenReturn("http://localhost:8080/");

        standardPropertyProvider.bindHttpService(reference1);
        assertNotNull(standardPropertyProvider);
        standardPropertyProvider.unbindHttpService(reference1);
    }
}
/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

import com.googlecode.junittoolbox.SuiteClasses
import org.junit.runner.RunWith
import sirius.kernel.ScenarioSuite

/**
 * Führt alle Tests aus.
 */
@RunWith(ScenarioSuite::class)
@SuiteClasses("**/*Test.class", "**/*Spec.class")
class TestSuite {

}

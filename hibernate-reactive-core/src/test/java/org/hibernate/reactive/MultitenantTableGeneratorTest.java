/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.annotations.EnabledFor;
import org.hibernate.reactive.provider.Settings;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.TableGenerator;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.MyCurrentTenantIdentifierResolver.Tenant.DEFAULT;
import static org.hibernate.reactive.MyCurrentTenantIdentifierResolver.Tenant.TENANT_1;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;

/**
 * The table generator requests a new block with a dedicated connection:
 * check that the connection is for the database of the tenant of the session.
 */
@Timeout(value = 10, timeUnit = MINUTES)
@EnabledFor(value = POSTGRESQL, reason = "The databases of the tenants are created with PostgreSQL scripts")
public class MultitenantTableGeneratorTest extends BaseReactiveTest {

	private static final MyCurrentTenantIdentifierResolver TENANT_RESOLVER = new MyCurrentTenantIdentifierResolver();

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( TenantTableId.class );
	}

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.setProperty( AvailableSettings.MULTI_TENANT_CONNECTION_PROVIDER, "anything" );
		configuration.getProperties().put( Settings.MULTI_TENANT_IDENTIFIER_RESOLVER, TENANT_RESOLVER );
		// Contains the SQL scripts for the creation of the additional databases
		configuration.setProperty( Settings.HBM2DDL_IMPORT_FILES, "/multitenancy-test.sql" );
		configuration.setProperty( Settings.SQL_CLIENT_POOL, TenantDependentPool.class.getName() );
		return configuration;
	}

	@Override
	protected CompletionStage<Void> cleanDb() {
		TENANT_RESOLVER.setTenantIdentifier( DEFAULT );
		return super.cleanDb();
	}

	@Test
	public void givenTenantWhenBlockIsRequestedThenTheTableOfTheTenantIsUpdated(VertxTestContext context) {
		final TenantTableId entity = new TenantTableId( "Tenant 1" );
		final AtomicReference<List<Long>> defaultValues = new AtomicReference<>();

		TENANT_RESOLVER.setTenantIdentifier( DEFAULT );
		test( context, generatorValues()
				.thenAccept( defaultValues::set )
				.thenAccept( v -> TENANT_RESOLVER.setTenantIdentifier( TENANT_1 ) )
				// The schema is only exported to the default database
				.thenCompose( v -> getSessionFactory().withTransaction( s -> s
						.createNativeQuery( "create table tenant_table_id (id bigint not null, string varchar(255), primary key (id))" )
						.executeUpdate()
						.thenCompose( vv -> s
								.createNativeQuery( "create table tenant_id_tab (sequence_name varchar(255) not null, next_val bigint, primary key (sequence_name))" )
								.executeUpdate() ) ) )
				.thenCompose( v -> getSessionFactory().withTransaction( s -> s.persist( entity ) ) )
				.thenCompose( v -> getSessionFactory().withSession( s -> s
						.createSelectionQuery( "from TenantTableId", TenantTableId.class )
						.getResultList() ) )
				.thenAccept( found -> assertThat( found ).containsExactly( entity ) )
				.thenCompose( v -> generatorValues() )
				.thenAccept( tenantValues -> assertThat( tenantValues ).hasSize( 1 ).doesNotContainNull() )
				.thenAccept( v -> TENANT_RESOLVER.setTenantIdentifier( DEFAULT ) )
				.thenCompose( v -> generatorValues() )
				.thenAccept( values -> assertThat( values ).isEqualTo( defaultValues.get() ) )
		);
	}

	private CompletionStage<List<Long>> generatorValues() {
		return getSessionFactory().withSession( s -> s
				.createNativeQuery( "select next_val from tenant_id_tab", Long.class )
				.getResultList() );
	}

	@Entity(name = "TenantTableId")
	@Table(name = "tenant_table_id")
	@TableGenerator(name = "tenantTab", table = "tenant_id_tab", allocationSize = 2)
	public static class TenantTableId {
		@Id
		@GeneratedValue(generator = "tenantTab")
		Long id;
		String string;

		public TenantTableId() {
		}

		public TenantTableId(String string) {
			this.string = string;
		}

		@Override
		public String toString() {
			return id + ": " + string;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			TenantTableId that = (TenantTableId) o;
			return Objects.equals( string, that.string );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( string );
		}
	}
}

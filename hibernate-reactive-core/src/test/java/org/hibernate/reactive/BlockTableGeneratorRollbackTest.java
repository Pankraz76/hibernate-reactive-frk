/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.hibernate.annotations.GenericGenerator;
import org.hibernate.annotations.Parameter;
import org.hibernate.id.enhanced.SequenceStyleGenerator;
import org.hibernate.reactive.stage.Stage;

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
import static org.hibernate.reactive.util.internal.CompletionStages.failedFuture;
import static org.hibernate.reactive.util.internal.CompletionStages.voidFuture;

/**
 * Given a generator that stores the next block of ids in a table,
 * when the transaction of the session that requested a block is rolled back
 * after another session has used an id of that block,
 * then the generator must not hand out the ids of that block again.
 * <p>
 * Like in Hibernate ORM, the generator updates the table in a separate transaction.
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class BlockTableGeneratorRollbackTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( TableGeneratorId.class, EmulatedSequenceId.class );
	}

	@Test
	public void givenTableGeneratorWhenSessionRequestingBlockRollsBackThenIdsAreNotReused(VertxTestContext context) {
		test( context, rollbackAfterOtherSessionUsedTheBlock( TableGeneratorId::new, TableGeneratorId.class ) );
	}

	@Test
	public void givenEmulatedSequenceWhenSessionRequestingBlockRollsBackThenIdsAreNotReused(VertxTestContext context) {
		test( context, rollbackAfterOtherSessionUsedTheBlock( EmulatedSequenceId::new, EmulatedSequenceId.class ) );
	}

	private <T> CompletionStage<Void> rollbackAfterOtherSessionUsedTheBlock(Function<String, T> factory, Class<T> entityClass) {
		final T rolledBack = factory.apply( "Rolled back, first id of the block" );
		final T committed = factory.apply( "Committed, second id of the block" );
		final T afterRollback1 = factory.apply( "After the rollback, first" );
		final T afterRollback2 = factory.apply( "After the rollback, second" );

		// With allocationSize = 2, the second session uses the id left in the block requested by the first one
		return inTransaction( first -> first
				.persist( rolledBack )
				.thenCompose( v -> first.flush() )
				.thenCompose( v -> inTransaction( second -> second.persist( committed ) ) )
				.thenCompose( v -> failedFuture( new RollbackRequested() ) ) )
				.handle( (v, throwable) -> {
					assertThat( throwable ).hasRootCauseInstanceOf( RollbackRequested.class );
					return null;
				} )
				.thenCompose( v -> inTransaction( third -> third
						.persist( afterRollback1 )
						.thenCompose( vv -> third.persist( afterRollback2 ) ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( s -> s.createSelectionQuery( "from " + entityClass.getSimpleName(), entityClass ).getResultList() )
				.thenAccept( found -> assertThat( found ).containsExactlyInAnyOrder( committed, afterRollback1, afterRollback2 ) );
	}

	// Each call needs its own session: SessionFactory#withTransaction would reuse the current one
	private CompletionStage<Void> inTransaction(Function<Stage.Session, CompletionStage<Void>> work) {
		return getSessionFactory().openSession().thenCompose( session -> session
				.withTransaction( tx -> work.apply( session ) )
				.handle( (v, throwable) -> throwable )
				.thenCompose( throwable -> session.close()
						.thenCompose( v -> throwable == null ? voidFuture() : failedFuture( throwable ) ) ) );
	}

	private static class RollbackRequested extends RuntimeException {
		RollbackRequested() {
			super( "Expected exception to roll back the transaction" );
		}
	}

	@Entity(name = "TableGeneratorId")
	@Table(name = "table_generator_id")
	@TableGenerator(name = "rollbackTab", table = "rollback_id_tab", allocationSize = 2)
	public static class TableGeneratorId {
		@Id
		@GeneratedValue(generator = "rollbackTab")
		Long id;
		String string;

		public TableGeneratorId() {
		}

		public TableGeneratorId(String string) {
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
			TableGeneratorId that = (TableGeneratorId) o;
			return Objects.equals( string, that.string );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( string );
		}
	}

	@Entity(name = "EmulatedSequenceId")
	@Table(name = "emulated_sequence_id")
	public static class EmulatedSequenceId {
		@Id
		@GeneratedValue
		@GenericGenerator(type = SequenceStyleGenerator.class, parameters = {
				@Parameter(name = SequenceStyleGenerator.SEQUENCE_PARAM, value = "rollback_id_seq"),
				@Parameter(name = SequenceStyleGenerator.FORCE_TBL_PARAM, value = "true"),
				@Parameter(name = SequenceStyleGenerator.INCREMENT_PARAM, value = "2")
		})
		Long id;
		String string;

		public EmulatedSequenceId() {
		}

		public EmulatedSequenceId(String string) {
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
			EmulatedSequenceId that = (EmulatedSequenceId) o;
			return Objects.equals( string, that.string );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( string );
		}
	}
}

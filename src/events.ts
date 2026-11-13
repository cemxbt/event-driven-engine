import { z } from 'zod';

/** Describes one kind of event: its name, the aggregate it belongs to and its payload. */
export interface EventDefinition<TType extends string = string, TPayload = unknown> {
  readonly type: TType;
  readonly aggregateType: string;
  readonly schema: z.ZodType<TPayload>;
}

export function defineEvent<TType extends string, TSchema extends z.ZodTypeAny>(
  type: TType,
  aggregateType: string,
  schema: TSchema,
): EventDefinition<TType, z.infer<TSchema>> {
  return { type, aggregateType, schema };
}

export type PayloadOf<D> = D extends EventDefinition<string, infer P> ? P : never;

export interface EventEnvelope<TPayload = unknown> {
  id: string;
  type: string;
  aggregateType: string;
  aggregateId: string;
  payload: TPayload;
  occurredAt: Date;
  /** Number of earlier delivery attempts that did not complete. */
  attempts: number;
}

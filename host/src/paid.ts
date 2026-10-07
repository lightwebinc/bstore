/**
 * The host's terms route (spec section 11.4): one small HTTP server beside
 * the overlay host, whose base URL is what the host publishes for
 * `ls_bstore`. It serves
 *
 *   GET  <base>/ls_bstore/terms      the host terms document (404 when the host prices nothing)
 *   POST <base>/.well-known/auth     the BRC-104 handshake
 *   POST <base>/lookup               BRC-24 questions to ls_bstore, free and priced
 *
 * The route, its hardening, the payee ledger and payment acceptance are
 * bcommon's shared host module (`@lightwebinc/bcommon/host`); this file
 * binds them to bstore: the service and its classes (spec section 11.1),
 * the settle protocol a payment pays under, the `bstore` metric and log
 * prefix, the payee ledger layout, and a session's response budget taken
 * before the request's signature is verified, so forged requests on one
 * session cost its budget and not the host's CPU. The provider routes of
 * spec section 10.1 (challenge, renew, settle, a provider's terms) are a
 * provider's, not a host's, and are not here.
 */
import {
  LookupFront as SharedFront,
  parsePrices as sharedPrices,
  termsDocument as sharedTerms,
  termsPath,
  type FrontOptions as SharedOptions,
  type LookupAnswerer,
} from '@lightwebinc/bcommon/host'
import type { LookupQuestion } from '@lightwebinc/bcommon'
import type { StoreLookupService } from './ls_bstore.js'
import { Classes } from './query.js'
import { LookupService as ServiceName, SettleProtocol } from './storerec.js'

export {
  BoundedSessions,
  DefaultMaxSessions,
  DefaultSessionTTL,
  HandshakeResults,
  LedgerReceiver,
  MemoryReceiver,
  PaymentVersion,
  RequestResults,
  TakenPayments,
  ledgerLine,
  spentOutpoints,
  type Claim,
  type PaymentReceiver,
  type ReceivedPayment,
} from '@lightwebinc/bcommon/host'

/** BRC-29's derivation protocol, under which a priced question's payment pays the host (spec section 11.4). */
export const PaymentProtocol = SettleProtocol as [2, string]

/** The terms route, below the base URL. */
export const TermsPath = termsPath(ServiceName)

/**
 * Parses a host's prices, `history=5,export=20,chain=10`: a comma list of
 * `<class>=<satoshis>`, each a priceable class of spec section 11.1 once,
 * priced 0 to 2^53 - 1.
 */
export const parsePrices = (raw: string | undefined): Map<string, number> => sharedPrices(raw, Classes, 'BSTORE_PRICES')

/** The host terms document (spec section 11.4), classes in the order of section 11.1. */
export const termsDocument = (prices: ReadonlyMap<string, number>): { service: string; terms: number; classes: Array<{ class: string; satoshis: number }> } =>
  sharedTerms(ServiceName, Classes, prices)

export type FrontOptions = Omit<SharedOptions, 'app' | 'service' | 'classes' | 'paymentProtocol' | 'ls' | 'responseBudget'> & { ls: StoreLookupService }

/** ls_bstore as the shared route asks it: a class by name, and an answer with its records hydrated. */
function answerer(ls: StoreLookupService): LookupAnswerer {
  return {
    get restored() {
      return ls.restored
    },
    classify: (q) => ls.classify(q as LookupQuestion).q.class,
    answer: async (q) => ls.hydrate(await ls.answer(q as LookupQuestion, true)),
  }
}

export class LookupFront extends SharedFront {
  constructor(o: FrontOptions) {
    super({ ...o, app: 'bstore', service: ServiceName, classes: Classes, paymentProtocol: PaymentProtocol, responseBudget: 'before-verify', ls: answerer(o.ls) })
  }
}

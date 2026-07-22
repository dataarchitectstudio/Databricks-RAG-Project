# Databricks notebook source
# MAGIC %pip install reportlab
# MAGIC %restart_python

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "insurance"
VOLUME = "policy_docs"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

print(f"Target volume path: {VOLUME_PATH}")

# COMMAND ----------

POLICIES = [
    {
        "filename": "policy_01_individual_basic.pdf",
        "policy_name": "SecureLife Individual Basic",
        "policy_number": "SL-IND-1001",
        "insurer": "SecureLife Health Insurance Co.",
        "coverage_type": "Individual",
        "sum_insured": "₹5,00,000",
        "premium": "₹8,500 / year",
        "waiting_periods": {
            "Initial waiting period": "30 days (except accidents)",
            "Pre-existing disease waiting period": "36 months",
            "Specific illness waiting period": "24 months (cataract, hernia, joint replacement)",
        },
        "inclusions": [
            "In-patient hospitalization (24+ hours)",
            "Pre-hospitalization expenses (30 days)",
            "Post-hospitalization expenses (60 days)",
            "Day-care procedures (150+ listed procedures)",
            "Ambulance charges up to ₹2,000 per hospitalization",
        ],
        "exclusions": [
            "Cosmetic or plastic surgery unless medically necessary",
            "Self-inflicted injuries",
            "Injuries from war, invasion, or nuclear risk",
            "Dental treatment unless due to accident",
            "Infertility and assisted reproduction treatment",
        ],
        "claim_process": [
            "Notify insurer within 24 hours of emergency admission",
            "Submit pre-authorization form for cashless claims",
            "For reimbursement, submit original bills within 30 days of discharge",
            "Claim settlement within 15 working days of complete documentation",
        ],
        "network_note": "Cashless treatment available at 4,500+ network hospitals nationwide.",
    },
    {
        "filename": "policy_02_family_floater.pdf",
        "policy_name": "SecureLife Family Shield Floater",
        "policy_number": "SL-FAM-2002",
        "insurer": "SecureLife Health Insurance Co.",
        "coverage_type": "Family Floater",
        "sum_insured": "₹10,00,000 (shared across family)",
        "premium": "₹18,200 / year",
        "waiting_periods": {
            "Initial waiting period": "30 days (except accidents)",
            "Pre-existing disease waiting period": "36 months",
            "Maternity waiting period": "24 months",
        },
        "inclusions": [
            "Covers self, spouse, and up to 3 dependent children",
            "Maternity cover after waiting period (normal + C-section)",
            "New-born baby cover from day 1 (for 90 days, then add-on required)",
            "Annual health check-up for all insured members",
            "Room rent up to single private AC room, no sub-limit",
        ],
        "exclusions": [
            "Congenital external diseases/defects",
            "Treatment for obesity/weight control unless medically necessary",
            "Non-allopathic treatment unless specifically opted",
            "Any claim arising within first 30 days except accidents",
        ],
        "claim_process": [
            "Cashless: Show e-card at network hospital, get pre-authorization",
            "Reimbursement: submit claim form + bills + discharge summary within 30 days",
            "Maternity claims require policy to be active 24+ months prior to delivery",
        ],
        "network_note": "Cashless treatment available at 5,200+ network hospitals nationwide.",
    },
    {
        "filename": "policy_03_senior_citizen.pdf",
        "policy_name": "SecureLife Senior Citizen Care",
        "policy_number": "SL-SEN-3003",
        "insurer": "SecureLife Health Insurance Co.",
        "coverage_type": "Senior Citizen",
        "sum_insured": "₹3,00,000",
        "premium": "₹22,750 / year",
        "waiting_periods": {
            "Initial waiting period": "30 days (except accidents)",
            "Pre-existing disease waiting period": "48 months",
            "Specific illness waiting period": "24 months",
        },
        "inclusions": [
            "Coverage for entrants aged 60-75 years",
            "Pre-existing conditions like diabetes, hypertension covered after waiting period",
            "Domiciliary hospitalization when hospital beds unavailable",
            "Co-payment: 20% on all claims (standard for senior citizen plans)",
            "AYUSH treatment covered up to sum insured",
        ],
        "exclusions": [
            "Any treatment taken outside India",
            "Unproven or experimental treatments",
            "Sterility, contraception, hormone replacement therapy",
            "Claims within first 30 days except accidental hospitalization",
        ],
        "claim_process": [
            "Dedicated senior citizen claim helpline (24x7)",
            "Cashless facility at network hospitals with pre-authorization",
            "Reimbursement claims to be filed within 30 days of discharge",
            "20% co-payment deducted from admissible claim amount",
        ],
        "network_note": "Cashless treatment available at 3,800+ network hospitals, includes dedicated senior-care desks.",
    },
    {
        "filename": "policy_04_critical_illness.pdf",
        "policy_name": "SecureLife Critical Illness Shield",
        "policy_number": "SL-CI-4004",
        "insurer": "SecureLife Health Insurance Co.",
        "coverage_type": "Critical Illness",
        "sum_insured": "₹15,00,000 (lump sum benefit)",
        "premium": "₹6,400 / year",
        "waiting_periods": {
            "Initial waiting period": "90 days from policy start",
            "Survival period": "30 days post-diagnosis required for payout",
        },
        "inclusions": [
            "Lump-sum payout on diagnosis of 20 listed critical illnesses",
            "Covers cancer, heart attack, stroke, kidney failure, major organ transplant",
            "No sub-limits — full sum insured paid on valid diagnosis",
            "Premium waiver for following year after a valid claim",
        ],
        "exclusions": [
            "Any critical illness diagnosed within first 90 days of policy",
            "Death within survival period of 30 days post-diagnosis",
            "Illnesses arising from alcohol or drug abuse",
            "Pre-existing critical illness diagnosed before policy inception",
        ],
        "claim_process": [
            "Submit diagnosis reports and specialist certification",
            "Claim payable only after 30-day survival period from diagnosis",
            "Lump sum paid directly to policyholder within 15 days of approval",
            "This is a benefit policy — no need to submit hospital bills",
        ],
        "network_note": "Not applicable — this is a fixed-benefit (lump sum) policy, not hospitalization-based.",
    },
    {
        "filename": "policy_05_maternity_plus.pdf",
        "policy_name": "SecureLife Maternity Plus",
        "policy_number": "SL-MAT-5005",
        "insurer": "SecureLife Health Insurance Co.",
        "coverage_type": "Maternity",
        "sum_insured": "₹4,00,000 (₹75,000 maternity sub-limit)",
        "premium": "₹14,900 / year",
        "waiting_periods": {
            "Initial waiting period": "30 days (except accidents)",
            "Maternity waiting period": "24 months from policy start",
            "Pre-existing disease waiting period": "36 months",
        },
        "inclusions": [
            "Normal delivery covered up to ₹50,000",
            "C-section delivery covered up to ₹75,000",
            "Pre-natal and post-natal expenses (60 days each)",
            "New-born baby cover included from day 1",
            "Vaccination cover for new-born up to 1 year",
        ],
        "exclusions": [
            "Maternity claims within first 24 months of policy",
            "Voluntary termination of pregnancy (unless medically indicated)",
            "Infertility treatment and IVF",
            "Maternity expenses for surrogate pregnancies",
        ],
        "claim_process": [
            "Notify insurer at least 48 hours before planned delivery",
            "Cashless available at network maternity hospitals",
            "Submit birth certificate and discharge summary for reimbursement",
            "New-born must be added to policy within 90 days of birth for continued cover",
        ],
        "network_note": "Cashless treatment available at 2,900+ network maternity/multi-specialty hospitals.",
    },
    {
        "filename": "policy_06_super_topup.pdf",
        "policy_name": "SecureLife Super Top-Up",
        "policy_number": "SL-TOP-6006",
        "insurer": "SecureLife Health Insurance Co.",
        "coverage_type": "Top-Up",
        "sum_insured": "₹20,00,000 (deductible ₹5,00,000)",
        "premium": "₹4,100 / year",
        "waiting_periods": {
            "Initial waiting period": "30 days (except accidents)",
            "Pre-existing disease waiting period": "36 months",
        },
        "inclusions": [
            "Covers hospitalization expenses above ₹5,00,000 deductible",
            "Deductible applies per policy year (aggregate, not per claim)",
            "Can be combined with any existing base health policy",
            "Covers same in-patient benefits as base policies once deductible is crossed",
        ],
        "exclusions": [
            "Claims below the ₹5,00,000 deductible threshold",
            "OPD expenses",
            "Non-medical items (as per standard exclusion list)",
            "Claims within first 30 days except accidents",
        ],
        "claim_process": [
            "Deductible must be exhausted via base policy or self-payment first",
            "Submit base policy claim settlement details along with top-up claim",
            "Reimbursement only — no cashless facility on this top-up plan",
            "Claim to be filed within 30 days of discharge",
        ],
        "network_note": "This is a reimbursement-only top-up plan; no direct network hospital cashless tie-up.",
    },
    {
        "filename": "policy_07_diabetes_care.pdf",
        "policy_name": "SecureLife Diabetes Safe",
        "policy_number": "SL-DIA-7007",
        "insurer": "SecureLife Health Insurance Co.",
        "coverage_type": "Chronic Illness (Diabetes)",
        "sum_insured": "₹5,00,000",
        "premium": "₹11,300 / year",
        "waiting_periods": {
            "Initial waiting period": "Nil — diabetes covered from day 1",
            "Diabetes-related complication waiting period": "12 months",
        },
        "inclusions": [
            "Designed specifically for pre-existing diabetic patients",
            "Diabetes and related complications covered from day 1 for base condition",
            "Annual HbA1c and diabetic health check-up included",
            "Insulin pump and consumables covered up to ₹25,000/year",
        ],
        "exclusions": [
            "Complications requiring surgery within first 12 months",
            "Non-diabetes related pre-existing conditions (standard 36-month wait applies)",
            "Cosmetic treatment of diabetes-related skin conditions",
        ],
        "claim_process": [
            "Diabetic history disclosure mandatory at policy purchase",
            "Cashless available for diabetes-related hospitalization",
            "Annual check-up claims processed via reimbursement only",
            "Claims for complications require 12-month continuous coverage proof",
        ],
        "network_note": "Cashless available at 3,100+ network hospitals with diabetology departments.",
    },
    {
        "filename": "policy_08_group_corporate.pdf",
        "policy_name": "SecureLife Corporate Group Health",
        "policy_number": "SL-GRP-8008",
        "insurer": "SecureLife Health Insurance Co.",
        "coverage_type": "Group / Corporate",
        "sum_insured": "₹8,00,000 (per employee)",
        "premium": "₹6,000 / year (employer-paid, per employee)",
        "waiting_periods": {
            "Initial waiting period": "Waived for group policies",
            "Pre-existing disease waiting period": "Waived for group policies",
        },
        "inclusions": [
            "No waiting periods — immediate coverage from date of joining",
            "Covers employee + spouse + up to 2 children",
            "Pre-existing diseases covered from day 1 (group policy benefit)",
            "Portability to individual policy on leaving the organization",
        ],
        "exclusions": [
            "Coverage ends on last working day unless converted to individual policy",
            "Cosmetic and elective procedures",
            "Non-network hospital treatment reimbursed at lower rate",
        ],
        "claim_process": [
            "HR/insurance desk coordinates cashless pre-authorization",
            "Reimbursement claims submitted via employer's HR portal",
            "Portability conversion request must be made within 45 days of exit",
        ],
        "network_note": "Cashless treatment available at 6,000+ network hospitals under corporate tie-up.",
    },
    {
        "filename": "policy_09_accident_care.pdf",
        "policy_name": "SecureLife Personal Accident Care",
        "policy_number": "SL-ACC-9009",
        "insurer": "SecureLife Health Insurance Co.",
        "coverage_type": "Personal Accident",
        "sum_insured": "₹10,00,000 (accidental death & disability)",
        "premium": "₹2,200 / year",
        "waiting_periods": {
            "Initial waiting period": "Nil — accident cover starts immediately",
        },
        "inclusions": [
            "100% sum insured on accidental death",
            "Up to 100% for permanent total disability",
            "Up to 50% for permanent partial disability (as per limb/organ chart)",
            "Weekly cash benefit during temporary total disability (up to 104 weeks)",
            "Child education benefit on death of insured",
        ],
        "exclusions": [
            "Death/injury from suicide or self-harm",
            "Injuries while under influence of alcohol or drugs",
            "Death/injury from participation in adventure sports (unless add-on opted)",
            "Pre-existing physical disabilities",
        ],
        "claim_process": [
            "FIR/medico-legal certificate required for accidental death claims",
            "Disability claims require certification from a civil surgeon",
            "Nominee to submit claim within 90 days of the incident",
            "Weekly cash benefit paid on submission of fitness-for-work certificates",
        ],
        "network_note": "Not applicable — this is a fixed-benefit accident policy, not hospitalization-based.",
    },
    {
        "filename": "policy_10_international_travel_health.pdf",
        "policy_name": "SecureLife Global Travel Health",
        "policy_number": "SL-INT-1010",
        "insurer": "SecureLife Health Insurance Co.",
        "coverage_type": "International Travel Health",
        "sum_insured": "USD 1,00,000",
        "premium": "USD 45 / trip (up to 30 days)",
        "waiting_periods": {
            "Initial waiting period": "Nil — cover starts from trip departure date",
        },
        "inclusions": [
            "Emergency hospitalization abroad",
            "Emergency medical evacuation and repatriation",
            "Trip cancellation/interruption due to medical emergency",
            "Loss of passport and baggage assistance",
            "24x7 international assistance helpline",
        ],
        "exclusions": [
            "Pre-existing conditions unless declared and approved",
            "Treatment in home country during covered trip",
            "Injuries from adventure/extreme sports (unless add-on opted)",
            "Travel to countries under active travel advisory/war zone",
        ],
        "claim_process": [
            "Contact 24x7 international assistance helpline immediately",
            "Submit original foreign hospital bills with certified translation if needed",
            "Claims settled in INR at prevailing exchange rate",
            "Claim must be filed within 30 days of return to home country",
        ],
        "network_note": "Direct billing available with international assistance network in 150+ countries.",
    },
]

print(f"Loaded {len(POLICIES)} policy definitions")

# COMMAND ----------

# Shared "General Conditions" boilerplate, common across all policy wordings
# (mirrors how real insurers structure policy documents: product-specific
# schedule + a large shared definitions/exclusions/T&C/FAQ section).

STANDARD_DEFINITIONS = [
    ("Accident", "A sudden, unforeseen, and involuntary event caused by external, violent, and visible means."),
    ("AYUSH Treatment", "Treatment under Ayurveda, Yoga, Naturopathy, Unani, Siddha, and Homeopathy systems."),
    ("Cashless Facility", "Facility where the insurer directly settles eligible hospital bills at a network provider."),
    ("Co-payment", "A cost-sharing requirement where the insured bears a specified percentage of the claim amount."),
    ("Congenital Anomaly", "A condition present since birth, which is abnormal with reference to form, structure, or position."),
    ("Day Care Treatment", "Medical treatment/surgery under anaesthesia requiring less than 24 hours of hospitalization due to technological advancement."),
    ("Deductible", "A fixed amount that the insured must bear before the insurer's liability begins for a claim."),
    ("Dependent Child", "A biological, legally adopted, or step child of the proposer, financially dependent on the primary insured."),
    ("Disclosure of Information", "The policy is issued on the basis of information provided by the proposer in the application form."),
    ("Emergency Care", "Management of a medical condition requiring immediate treatment to avoid serious deterioration."),
    ("Grace Period", "The specified period after the premium due date during which the policy is deemed to be in force without a break."),
    ("Hospital", "An institution registered as a hospital with the local authorities, with minimum inpatient beds and qualified staff."),
    ("Hospitalization", "Admission in a hospital for a minimum period of 24 consecutive hours for inpatient care."),
    ("ICU Charges", "Expenses incurred for accommodation in the intensive care unit, including nursing and monitoring charges."),
    ("In-patient Care", "Treatment for which the insured has to stay in a hospital for more than 24 hours."),
    ("Maternity Expenses", "Expenses on hospitalization for delivery, including caesarean section, and lawful medical termination."),
    ("Migration", "The facility allowing a policyholder to move to another policy offered by the same insurer."),
    ("Network Provider", "Hospitals or healthcare providers enlisted by the insurer to provide cashless treatment."),
    ("Notification of Claim", "The process of intimating a claim to the insurer through the defined channels within the specified time."),
    ("OPD Treatment", "Treatment where the insured visits the clinic/hospital but is not admitted as an inpatient."),
    ("Portability", "The facility to transfer credit gained for waiting periods when switching insurers."),
    ("Pre-existing Disease", "Any condition, ailment, or injury diagnosed within 48 months prior to the policy's effective date."),
    ("Pre-hospitalization Expenses", "Medical expenses incurred during a defined period prior to hospitalization, for the same condition."),
    ("Post-hospitalization Expenses", "Medical expenses incurred during a defined period after discharge, for the same condition."),
    ("Reasonable and Customary Charges", "Charges for services consistent with those normally charged for similar services in the locality."),
    ("Renewal", "The terms on which the contract of insurance can be continued beyond the original policy period."),
    ("Room Rent", "The amount charged by a hospital for occupancy of a bed, including associated medical expenses."),
    ("Sub-limit", "A cap on the amount of coverage available for specified illnesses/procedures/expenses under a policy."),
    ("Sum Insured", "The maximum amount the insurer will pay in respect of claims during the policy period."),
    ("Waiting Period", "A time period during which specified illnesses or conditions are not covered by the policy."),
]

STANDARD_PERMANENT_EXCLUSIONS = [
    "Investigation & Evaluation: Admission primarily for diagnostic purposes without positive findings.",
    "Rest cure, rehabilitation, and respite care not requiring active medical treatment.",
    "Obesity/weight control treatment unless medically necessary and pre-authorized.",
    "Change-of-gender treatments and related surgeries.",
    "Cosmetic or plastic surgery unless required for reconstruction after an accident or cancer.",
    "Hazardous or adventure sports including but not limited to bungee jumping, para-sailing, skiing.",
    "Breach of law with criminal intent by the insured person.",
    "Excluded providers: treatment received from any excluded list of providers as notified by the insurer.",
    "Treatment for alcoholism, drug or substance abuse, or addictive conditions.",
    "Treatments received outside India unless specifically covered under an add-on.",
    "Refractive error correction below 7.5 dioptres.",
    "Unproven and experimental treatment not recognized by medical science.",
    "Maternity expenses except as specifically covered under a maternity benefit or add-on.",
    "Sterility and infertility treatments including assisted reproduction.",
    "Dietary supplements and substances that can be purchased without a prescription.",
    "Any expenses for donor screening or organ transplant donor's post-operative complications.",
    "War, invasion, act of foreign enemy, hostilities, civil war, rebellion, or nuclear risk.",
    "Genetic disorders and stem cell therapy unless specifically covered.",
    "Non-allopathic treatment unless the insured has specifically opted for AYUSH cover.",
    "Any claim arising within the initial waiting period, except for accidental hospitalization.",
]

STANDARD_TERMS_AND_CONDITIONS = [
    ("Free-look Period", "The policyholder has 15 days from receipt of the policy document to review the terms. "
        "If not satisfied, the policy can be cancelled for a refund, subject to deduction of proportionate risk premium and any medical examination costs."),
    ("Grace Period", "A grace period of 15 days (for monthly mode) or 30 days (for other modes) is allowed for renewal premium payment, "
        "during which coverage continuity is maintained without a fresh waiting period."),
    ("Renewal Terms", "The policy is renewable for life, subject to timely payment of renewal premium. "
        "The insurer reserves the right to revise premium, terms, and conditions on renewal with prior regulatory approval."),
    ("Migration and Portability", "Policyholders have the right to migrate to another product of the same insurer or port to another insurer, "
        "retaining continuity benefits for waiting periods already served, as per applicable regulatory guidelines."),
    ("Moratorium Period", "After completion of 60 continuous months of coverage, no claim shall be contestable except on grounds of proven fraud "
        "or permanent exclusions specified in the policy."),
    ("Cancellation", "The policyholder may cancel the policy by giving written notice; refund of premium for the unexpired policy period will be made "
        "on a short-period scale, provided no claim has been made during the policy period."),
    ("Disclosure Norms", "The policy is a contract of utmost good faith (uberrima fides) and is issued on the basis of accurate disclosure of information "
        "by the proposer. Misrepresentation or non-disclosure may result in claim repudiation or policy cancellation."),
    ("Claims Settlement", "All claims under the policy shall be settled or repudiated within 30 days of receipt of the last necessary document, "
        "in accordance with applicable regulatory timelines."),
    ("Multiple Policies", "In case the insured holds multiple health policies, they may choose the insurer from whom the claim is to be made, "
        "up to the available sum insured, and may claim balance amounts from other insurers."),
    ("Automatic Restoration of Sum Insured", "In the event the sum insured is exhausted during the policy year due to claims, "
        "it may be restored once, subject to policy-specific terms, for subsequent unrelated claims."),
    ("Nomination", "The policyholder may nominate a person to receive policy benefits in the event of the policyholder's death, "
        "and may change the nomination at any time during the policy period."),
    ("Tax Benefit", "Premiums paid under this policy may be eligible for tax deduction under Section 80D of the Income Tax Act, "
        "subject to the provisions of prevailing tax laws."),
]

STANDARD_FAQ = [
    ("Can I add family members after purchasing the policy?", "Family members can typically be added only at renewal, "
        "except for a new-born child who can be added mid-term within the specified enrolment window."),
    ("What happens if I miss my renewal premium payment?", "You get a grace period after the due date to renew without losing continuity benefits; "
        "coverage lapses if premium is not paid within the grace period."),
    ("Is COVID-19 treatment covered?", "Yes, hospitalization for COVID-19 and related complications is covered like any other illness, "
        "subject to policy terms and applicable waiting periods."),
    ("Can I port this policy from another insurer?", "Yes, portability is allowed as per regulatory guidelines; "
        "prior policy waiting periods already served will be credited, subject to submission of continuity proof."),
    ("Are pre-existing diseases covered?", "Pre-existing diseases are covered after completion of the specified pre-existing disease waiting period, "
        "provided they were disclosed at the time of proposal."),
    ("What documents are needed for a reimbursement claim?", "Duly filled claim form, original hospital bills, discharge summary, "
        "investigation reports, and prescriptions are typically required."),
    ("Does the policy cover alternative treatments like Ayurveda?", "AYUSH treatment is covered up to the sum insured if taken at a "
        "government-recognized or accredited AYUSH hospital."),
    ("Is there a limit on room rent?", "Some policies specify a room category or percentage-of-sum-insured cap on room rent; "
        "exceeding this may result in proportionate deduction on associated charges."),
    ("Can I cancel the policy mid-term?", "Yes, subject to a short-period cancellation scale and provided no claim has been made in that policy year."),
    ("How is the claim settlement amount calculated?", "Admissible expenses are assessed as per policy terms, sub-limits, deductibles, "
        "and co-payment clauses, with the balance recoverable from the insurer up to the sum insured."),
    ("Does the policy renew automatically?", "Renewal is not automatic; the policyholder must pay the renewal premium before "
        "or within the grace period after the due date to maintain continuous coverage."),
    ("What is not covered under any circumstance?", "Permanent exclusions such as cosmetic surgery, self-inflicted injury, and war-related "
        "claims are never covered, regardless of waiting period completion."),
    ("Can the sum insured be increased later?", "Sum insured enhancement requests are usually considered at renewal, "
        "subject to underwriting and may attract a fresh waiting period for the increased portion."),
    ("Is there a co-payment for all age groups?", "Co-payment, if applicable, is typically defined in the policy schedule and may vary "
        "by entry age band, especially for senior citizen-oriented products."),
    ("How do I locate a network hospital?", "A list of network hospitals is available on the insurer's website and mobile app, "
        "and can also be obtained via the 24x7 customer helpline."),
]

DAY_CARE_PROCEDURES = [
    "Cataract surgery", "Tonsillectomy", "Dilatation & curettage", "Lithotripsy (kidney stone removal)",
    "Haemodialysis", "Chemotherapy", "Radiotherapy", "Coronary angiography", "Coronary angioplasty",
    "Fracture reduction (excluding hairline fracture)", "Hydrocele surgery", "Appendectomy",
    "Sinus surgery", "Skin grafting", "Excision of cyst/lipoma", "Nasal cauterization",
    "Eye surgery (excluding cataract)", "Ear surgery (Myringotomy)", "Colonoscopy with biopsy",
    "Endoscopic procedures (upper GI)", "Removal of foreign body under anaesthesia", "Circumcision (medically indicated)",
    "Varicose vein stripping", "Fissurectomy", "Fistulectomy", "Hemorrhoidectomy (Piles)",
    "Prostate surgery (TURP)", "Hysterectomy (specific cases)", "Cyst removal (ovarian)",
    "Arthroscopic knee surgery", "Carpal tunnel release", "Cervical biopsy", "Breast lump excision",
    "Liver abscess drainage", "Nasal polypectomy", "Tympanoplasty", "Angiography (peripheral)",
    "Dental surgery (accident-related)", "Radiofrequency ablation", "Laser eye surgery",
    "Intraocular lens implant",
]


def format_procedure_benefit_table(policy: dict) -> list:
    """Generates a procedure-wise sub-limit table for a policy from a shared list."""
    rows = [["Procedure / Condition", "Sub-limit / Coverage Basis"]]
    for i, proc in enumerate(DAY_CARE_PROCEDURES):
        if i % 5 == 0:
            note = "Up to 10% of Sum Insured"
        elif i % 5 == 1:
            note = "Up to 15% of Sum Insured"
        elif i % 5 == 2:
            note = "Covered up to actuals (network hospital)"
        elif i % 5 == 3:
            note = "Up to 20% of Sum Insured per event"
        else:
            note = "As per Reasonable and Customary Charges"
        rows.append([proc, note])
    return rows


print(f"Loaded {len(POLICIES)} policy definitions")
print(f"Shared boilerplate: {len(STANDARD_DEFINITIONS)} definitions, "
      f"{len(STANDARD_PERMANENT_EXCLUSIONS)} standard exclusions, "
      f"{len(STANDARD_TERMS_AND_CONDITIONS)} T&C clauses, "
      f"{len(STANDARD_FAQ)} FAQs, {len(DAY_CARE_PROCEDURES)} day-care procedures")

# COMMAND ----------

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, ListFlowable, ListItem
)
import io

styles = getSampleStyleSheet()
title_style = ParagraphStyle("PolicyTitle", parent=styles["Title"], fontSize=18, spaceAfter=6)
heading_style = ParagraphStyle("SectionHeading", parent=styles["Heading2"], spaceBefore=14, spaceAfter=6)
body_style = styles["BodyText"]


def build_policy_pdf(policy: dict) -> bytes:
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer, pagesize=A4,
        topMargin=2*cm, bottomMargin=2*cm, leftMargin=2*cm, rightMargin=2*cm
    )
    story = []

    story.append(Paragraph(policy["policy_name"], title_style))
    story.append(Paragraph(f"Insurer: {policy['insurer']}", body_style))
    story.append(Spacer(1, 12))

    details_data = [
        ["Policy Number", policy["policy_number"]],
        ["Coverage Type", policy["coverage_type"]],
        ["Sum Insured", policy["sum_insured"]],
        ["Premium", policy["premium"]],
    ]
    details_table = Table(details_data, colWidths=[5*cm, 10*cm])
    details_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (0, -1), colors.whitesmoke),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
    ]))
    story.append(details_table)

    story.append(Paragraph("Waiting Periods", heading_style))
    wp_rows = [[k, v] for k, v in policy["waiting_periods"].items()]
    wp_table = Table(wp_rows, colWidths=[7*cm, 8*cm])
    wp_table.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    story.append(wp_table)

    story.append(Paragraph("Inclusions / What's Covered", heading_style))
    story.append(ListFlowable(
        [ListItem(Paragraph(i, body_style)) for i in policy["inclusions"]],
        bulletType="bullet"
    ))

    story.append(Paragraph("Exclusions / What's Not Covered", heading_style))
    story.append(ListFlowable(
        [ListItem(Paragraph(e, body_style)) for e in policy["exclusions"]],
        bulletType="bullet"
    ))

    story.append(Paragraph("Claim Process", heading_style))
    story.append(ListFlowable(
        [ListItem(Paragraph(s, body_style)) for s in policy["claim_process"]],
        bulletType="1"
    ))

    story.append(Paragraph("Network Hospitals", heading_style))
    story.append(Paragraph(policy["network_note"], body_style))

    story.append(Paragraph("Definitions", heading_style))
    for term, defn in STANDARD_DEFINITIONS:
        story.append(Paragraph(f"<b>{term}:</b> {defn}", body_style))
        story.append(Spacer(1, 4))

    story.append(Paragraph("Day Care Procedures - Sub-limits", heading_style))
    proc_table = Table(format_procedure_benefit_table(policy), colWidths=[9*cm, 6*cm])
    proc_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    story.append(proc_table)

    story.append(Paragraph("Standard Permanent Exclusions", heading_style))
    story.append(ListFlowable(
        [ListItem(Paragraph(e, body_style)) for e in STANDARD_PERMANENT_EXCLUSIONS],
        bulletType="bullet"
    ))

    story.append(Paragraph("Terms and Conditions", heading_style))
    for heading, text in STANDARD_TERMS_AND_CONDITIONS:
        story.append(Paragraph(f"<b>{heading}:</b> {text}", body_style))
        story.append(Spacer(1, 4))

    story.append(Paragraph("Frequently Asked Questions", heading_style))
    for q, a in STANDARD_FAQ:
        story.append(Paragraph(f"<b>Q: {q}</b>", body_style))
        story.append(Paragraph(f"A: {a}", body_style))
        story.append(Spacer(1, 4))

    story.append(Paragraph("Grievance Redressal", heading_style))
    story.append(Paragraph(
        f"For any grievance regarding {policy['policy_name']}, policyholders may contact the insurer's "
        "customer care, escalate to the Grievance Redressal Officer if unresolved within 15 days, and "
        "subsequently approach the Insurance Ombudsman as per IRDAI guidelines if still unsatisfied.",
        body_style
    ))

    story.append(Spacer(1, 20))
    story.append(Paragraph(
        "This is a sample/demo policy document generated for testing purposes only. "
        "Not a real insurance product.", styles["Italic"]
    ))

    doc.build(story)
    return buffer.getvalue()


print("PDF builder function ready")

# COMMAND ----------

manifest = []
for policy in POLICIES:
    pdf_bytes = build_policy_pdf(policy)
    out_path = f"{VOLUME_PATH}/{policy['filename']}"
    with open(out_path, "wb") as f:
        f.write(pdf_bytes)
    manifest.append({
        "filename": policy["filename"],
        "policy_name": policy["policy_name"],
        "policy_number": policy["policy_number"],
        "coverage_type": policy["coverage_type"],
        "size_bytes": len(pdf_bytes),
    })
    print(f"Wrote {out_path} ({len(pdf_bytes)} bytes)")

# COMMAND ----------

manifest_df = spark.createDataFrame(manifest)
display(manifest_df)

print(f"\nDone. {len(manifest)} policy PDFs written to {VOLUME_PATH}")

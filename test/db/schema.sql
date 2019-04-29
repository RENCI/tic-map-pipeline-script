--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: Administrator; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "Administrator" (
    "userId" character varying,
    "adminName" character varying,
    "adminEmail" character varying
);


ALTER TABLE public."Administrator" OWNER TO postgres;

--
-- Name: AssignProposal; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "AssignProposal" (
    "ProposalID" bigint,
    "assignToInstitution" character varying,
    "ticPOC" character varying,
    "ricPOC" character varying,
    "ncatsPOC" character varying
);


ALTER TABLE public."AssignProposal" OWNER TO postgres;

--
-- Name: BudgetBreakOut; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "BudgetBreakOut" (
    "ProposalID" bigint,
    "siteBudget" character varying,
    "recruitmentBudget" character varying,
    "overallBudget" character varying,
    "budgetNotes" character varying
);


ALTER TABLE public."BudgetBreakOut" OWNER TO postgres;

--
-- Name: CTSA; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "CTSA" (
    "CTSAhubPIFirstName" character varying,
    "CTSAhubPILastName" character varying,
    "ProposalID" bigint,
    "approvalfromCTSA" boolean
);


ALTER TABLE public."CTSA" OWNER TO postgres;

--
-- Name: ConsultationRequest; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "ConsultationRequest" (
    "consultationRequestID" bigint,
    "ProposalID" bigint,
    "serviceOrComprehensive" character varying
);


ALTER TABLE public."ConsultationRequest" OWNER TO postgres;

--
-- Name: FinalRecommendation; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "FinalRecommendation" (
    "ProposalID" bigint,
    recommendation character varying,
    "serviceRecommended" character varying,
    "IsServiceFeasible" boolean,
    "hoursToOperationalizeService" character varying,
    "howOperationalizeService" character varying
);


ALTER TABLE public."FinalRecommendation" OWNER TO postgres;

--
-- Name: InitialConsultationDates; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "InitialConsultationDates" (
    "ProposalID" bigint,
    "FirstContact" date,
    "kickOffNeeded" boolean,
    "kickOffScheduled" date,
    "kickOffDateOccurs" date,
    "workComplete" date,
    "reportSentToPI" date
);


ALTER TABLE public."InitialConsultationDates" OWNER TO postgres;

--
-- Name: InitialConsultationSummary; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "InitialConsultationSummary" (
    "ProposalID" bigint,
    "protocolReviewed" boolean,
    "budgetReviewed" boolean,
    "fundingReviewed" boolean,
    "CIRBdiscussed" boolean,
    "SAdiscussed" boolean,
    "EHRdiscussed" boolean,
    "CommunityEngagementDiscuss" boolean,
    "RecruitmentPlanDiscussed" boolean,
    "FeasibilityAssessmentDiscussed" boolean,
    "OtherComments" character varying
);


ALTER TABLE public."InitialConsultationSummary" OWNER TO postgres;

--
-- Name: LettersAndSurvey; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "LettersAndSurvey" (
    "ProposalID" bigint,
    "decisionLetterSent" date,
    "satisfactionSurveySent" date,
    "LetterOfSupport" date
);


ALTER TABLE public."LettersAndSurvey" OWNER TO postgres;

--
-- Name: PATMeeting; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "PATMeeting" (
    "ProposalID" bigint,
    "meetingDate" date,
    "meetingNumber" bigint,
    "approvedFor" character varying,
    comments character varying
);


ALTER TABLE public."PATMeeting" OWNER TO postgres;

--
-- Name: PATReviewForVote; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "PATReviewForVote" (
    "ProposalID" bigint,
    "requireVote" character varying,
    vote character varying
);


ALTER TABLE public."PATReviewForVote" OWNER TO postgres;

--
-- Name: Proposal; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "Proposal" (
    "ProposalID" bigint,
    "dateSubmitted" date,
    "proposalStatus" character varying,
    "HEALnetwork" boolean,
    "ShareThisInfo" boolean,
    "FullTitle" character varying,
    "ShortTitle" character varying,
    "PhaseOfStudy" character varying,
    "ShortDescription" character varying,
    "ProtocolDesign" character varying,
    "Objectives" character varying,
    "Endpoints" character varying,
    "StudyPopulation" character varying,
    "MainEntryCriteria" character varying,
    "PlannedSitesEnrollingParticipants" character varying,
    "DescriptionOfStudyIntervention" character varying,
    "StudyDuration" character varying,
    "ParticipantDuration" character varying,
    "DisclosureConflicts" character varying,
    "optStatisticalPlan" character varying,
    "optEnrollmentPlan" character varying
);


ALTER TABLE public."Proposal" OWNER TO postgres;

--
-- Name: ProposalDetails; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "ProposalDetails" (
    "ProposalID" bigint,
    "therapeuticArea" character varying,
    "rareDisease" boolean,
    "numberSubjects" bigint,
    "studyPopulation" character varying,
    "numberSites" bigint,
    "numberNonUSsites" bigint,
    "listCountries" character varying,
    "numberCTSAprogHubSites" bigint
);


ALTER TABLE public."ProposalDetails" OWNER TO postgres;

--
-- Name: ProposalFunding; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "ProposalFunding" (
    "ProposalID" bigint,
    "submittedToNIH" boolean,
    "currentFunding" character varying,
    "newFundingStatus" character varying,
    "numberFundingSource" bigint,
    "fundingSource" character varying,
    "fundingMechanism" character varying,
    "identifyFundingMechanism" character varying,
    "instituteCenter" character varying,
    "grantApplicationNumber" character varying,
    "FOAnumber" character varying,
    "planningGrant" boolean,
    "largerThan500K" boolean,
    "totalBudget" character varying,
    "fundingPeriod" character varying,
    "fundingStart" date,
    "applicationToInstituteBusinessOfficeDate" date,
    "discussWithPO" boolean,
    "POsName" character varying,
    "NewOrExistingNetwork" boolean,
    "peerReviewDone" character varying
);


ALTER TABLE public."ProposalFunding" OWNER TO postgres;

--
-- Name: Proposal_NewServiceSelection; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "Proposal_NewServiceSelection" (
    "ProposalID" bigint,
    "serviceSelection" character varying
);


ALTER TABLE public."Proposal_NewServiceSelection" OWNER TO postgres;

--
-- Name: Proposal_ServicesApproved; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "Proposal_ServicesApproved" (
    "ProposalID" bigint,
    "servicesApproved" character varying
);


ALTER TABLE public."Proposal_ServicesApproved" OWNER TO postgres;

--
-- Name: ProtocolTimelines_estimated; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "ProtocolTimelines_estimated" (
    "ProposalID" bigint,
    "plannedFinalProtocol" date,
    "plannedFirstSiteActivated" date,
    "plannedSubmissionDate" date,
    "plannedGrantSubmissionDate" date,
    "actualGrantSubmissionDate" date,
    "plannedGrantAwardDate" date,
    "actualGrantAwardDate" date,
    "estimatedStartDateOfFunding" date
);


ALTER TABLE public."ProtocolTimelines_estimated" OWNER TO postgres;

--
-- Name: RecommendationsForPI; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "RecommendationsForPI" (
    "ProposalID" bigint,
    "protocolRecommendation" character varying,
    "budgetRecommendation" boolean,
    "fundingAssessment" character varying,
    "CIRBrecommendation" character varying,
    "SArecommendation" character varying
);


ALTER TABLE public."RecommendationsForPI" OWNER TO postgres;

--
-- Name: ServicesAdditionalInfo; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "ServicesAdditionalInfo" (
    "consultationRequestID" bigint,
    "SAuseBefore" boolean,
    "CIRBfwaNumber" character varying
);


ALTER TABLE public."ServicesAdditionalInfo" OWNER TO postgres;

--
-- Name: SiteInformation; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "SiteInformation" (
    "ProposalID" bigint,
    "siteNumber" bigint,
    "siteName" character varying,
    "principleInvestigator" character varying,
    "studyCoordinator" character varying,
    "ctsaName" character varying,
    "ctsaPOC" character varying,
    "activeProtocolDate" date,
    "protocolVersion" character varying,
    "projectedEnrollmentPerMonth" bigint,
    "IRBOriginalApproval" date,
    "CTA_FE" date,
    "enrollmentStatus" character varying,
    "onHoldDate" date,
    "onHoldDays" bigint,
    "siteActivatedDate" date,
    "dateOfFirstConsent" date,
    "dateOfFirstPtEnrolled" date,
    "mostRecentConsent" date,
    "mostRecentEnrolled" date,
    "noOfPtsSignedConsent" bigint,
    "noOfPtsEnrolled_site" bigint,
    "noOfPtsActive_site" bigint,
    "noOfPtsComplete_site" bigint,
    "noOfPtsWithdrawn_site" bigint,
    "noOfCRFsCompleted_site" bigint,
    "percentCRFsReviewed_site" bigint,
    "percentCRFsIncomplete_site" bigint,
    "noOfUnresolvedQueries_site" bigint,
    "noOfSAEs_site" bigint,
    "noOfSignificantProtocolDeviations_site" bigint,
    "CTAsentdate" date,
    "regPacksentdate" date,
    "siteSelectDate" date,
    "notesToSite" character varying
);


ALTER TABLE public."SiteInformation" OWNER TO postgres;

--
-- Name: StudyInformation; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "StudyInformation" (
    "ProposalID" bigint,
    "studyStartDate" date,
    "plannedCompleteEnrollment" date,
    "noOfSitesActive" bigint,
    "noOfPtsEnrolled" bigint,
    "noOfPtsActive" bigint,
    "noOfPtsComplete" bigint,
    "noOfPtsWithdrawn" bigint,
    "noOfCRFsEnteredForStudy" bigint,
    "percentCRFsReviewed" bigint,
    "percentCRFsIncomplete" bigint,
    "noOfUnresolvedQueries" bigint,
    "noOfSignificantProtocolDeviations" bigint,
    "noOfSAEs" bigint,
    "mostRecentPatientEnrolled" date
);


ALTER TABLE public."StudyInformation" OWNER TO postgres;

--
-- Name: StudyPI; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "StudyPI" (
    "AreYouStudyPI" boolean,
    "userId" bigint
);


ALTER TABLE public."StudyPI" OWNER TO postgres;

--
-- Name: Submitter; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "Submitter" (
    "userId" character varying,
    "ProposalID" bigint,
    "submitterFirstName" character varying,
    "submitterLastName" character varying,
    "submitterFacultyStatus" bigint,
    "submitterEmail" character varying,
    "submitterPhone" character varying,
    "submitterInstitution" character varying
);


ALTER TABLE public."Submitter" OWNER TO postgres;

--
-- Name: SuggestedChanges; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "SuggestedChanges" (
    "changeID" bigint,
    "ShortTitle" character varying,
    "plannedDateToChange" date,
    "changeComplete" boolean
);


ALTER TABLE public."SuggestedChanges" OWNER TO postgres;

--
-- Name: TIC_RICAssessment; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "TIC_RICAssessment" (
    "ProposalID" bigint,
    "Issues" character varying,
    "BudgetFeasible" character varying,
    "TICcapacity" boolean,
    "undertakeAtCurrentState" character varying,
    "opportunityToCollaborate" character varying,
    "operationHypothesis" character varying
);


ALTER TABLE public."TIC_RICAssessment" OWNER TO postgres;

--
-- Name: TIChealPOCs; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "TIChealPOCs" (
    "ProposalID" bigint,
    "DukePOC" character varying,
    "UtahPOC" character varying,
    "jhuPOC" character varying
);


ALTER TABLE public."TIChealPOCs" OWNER TO postgres;

--
-- Name: TINuser; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "TINuser" (
    "userId" character varying,
    "TINuser_fname" character varying,
    "TINuser_lname" character varying,
    "TINuser_email" character varying,
    "TINuserOrganization" character varying
);


ALTER TABLE public."TINuser" OWNER TO postgres;

--
-- Name: User; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "User" (
    "userId" character varying,
    password character varying,
    "loginStatus" character varying,
    "registerDate" date
);


ALTER TABLE public."User" OWNER TO postgres;

--
-- Name: UtahRecommendation; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "UtahRecommendation" (
    "ProposalID" bigint,
    network character varying,
    tic character varying,
    ric character varying,
    "collaborativeTIC" character varying,
    "collaborativeTIC_roleExplain" character varying,
    "DCCinstitution" character varying,
    "CCCinstitution" character varying,
    "primaryStudyType" character varying,
    "sub_ancillaryStudy" boolean,
    "mainStudy" character varying,
    "hasSubAncillaryStudy" boolean,
    "sub_ancillaryStudyName" character varying,
    "linkedData" character varying,
    "studyDesign" character varying,
    randomized boolean,
    "randomizationUnit" character varying,
    "randomizationFeature" character varying,
    ascertainment character varying,
    observations character varying,
    "pilot_demoStudy" boolean,
    pilot_or_demo character varying,
    registry boolean,
    "EHRdataTransfer" boolean,
    "EHRdataTransfer_option" character varying,
    consent boolean,
    "EFIC" boolean,
    "IRBtype" character varying,
    "regulatoryClassification" character varying,
    "clinicalTrialsIdentifier" character varying,
    "dsmb_dmcUsed" boolean,
    "initialPlannedNumberOfSites" bigint,
    "finalPlannedNumberOfSites" bigint,
    "enrollmentGoal" character varying,
    "initialProjectedEnrollmentDuration" bigint,
    "actualEnrollment" bigint
);


ALTER TABLE public."UtahRecommendation" OWNER TO postgres;

--
-- Name: Voter; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "Voter" (
    "userId" character varying,
    "ProposalID" bigint,
    "Role" character varying
);


ALTER TABLE public."Voter" OWNER TO postgres;

--
-- Name: name; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE name (
    "table" character varying,
    "column" character varying,
    index character varying,
    id character varying,
    description character varying
);


ALTER TABLE public.name OWNER TO postgres;

--
-- Name: reviewer_organization; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE reviewer_organization (
    reviewer character varying,
    organization character varying
);


ALTER TABLE public.reviewer_organization OWNER TO postgres;

--
-- Name: public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

